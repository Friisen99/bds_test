using Orleans.Streams;
using Utilities;
using SocialNetwork;
using System.Diagnostics;
using System.Net.WebSockets;

namespace Library.Stream;

public interface IWindowJoinOperator : IGrainWithStringKey
{
    // you should not change this interface
    Task Init(IAsyncStream<Event> inputStream1, IAsyncStream<Event> inputStream2, IAsyncStream<Event> outputStream, int windowSlide, int windowLength);
}

internal sealed class WindowJoinOperator : Grain, IWindowJoinOperator
{
    int windowSlide;
    int windowLength;
    IAsyncStream<Event> outputStream;

    long[] maxReceivedWatermark;

    // you can add more data structures here
    // ... ... ...
    private Dictionary<long, List<Event>> tagEvents = new Dictionary<long, List<Event>>();
    private Dictionary<long, List<Event>> likeEvents = new Dictionary<long, List<Event>>();
    private int joinedCount = 0; // TODO: remove

    public async Task Init(IAsyncStream<Event> inputStream1, IAsyncStream<Event> inputStream2, IAsyncStream<Event> outputStream, int windowSlide, int windowLength)
    {
        this.windowSlide = windowSlide;
        this.windowLength = windowLength;
        Debug.Assert(windowLength % windowSlide == 0);
        this.outputStream = outputStream;
        
        maxReceivedWatermark = new long[2] { Constants.initialWatermark, Constants.initialWatermark };

        // whenever the operator receives an event, the method "ProcessEvent" is called automatically
        await inputStream1.SubscribeAsync(ProcessEvent1);
        await inputStream2.SubscribeAsync(ProcessEvent2);
    }

    async Task ProcessEvent1(Event e, StreamSequenceToken _)
    {
        switch (e.type)
        {
            case EventType.Regular:
                Debug.Assert(e.timestamp > maxReceivedWatermark[0]);
                await ProcessRegularEvent(e, 1);
                break;
            case EventType.Watermark:
                maxReceivedWatermark[0] = Math.Max(maxReceivedWatermark[0], e.timestamp);
                await ProcessWatermark();
                break;
            default:
                throw new Exception($"Exception event type {e.type} is not supported. ");
        }
    }

    async Task ProcessEvent2(Event e, StreamSequenceToken _)
    {
        switch (e.type)
        {
            case EventType.Regular:
                Debug.Assert(e.timestamp > maxReceivedWatermark[1]);
                await ProcessRegularEvent(e, 2);
                break;
            case EventType.Watermark:
                maxReceivedWatermark[1] = Math.Max(maxReceivedWatermark[1], e.timestamp);
                await ProcessWatermark();
                break;
            default:
                throw new Exception($"Exception event type {e.type} is not supported. ");
        }
    }

    async Task ProcessWatermark()
    {
        // process watermark for each stream
        ProcessStreamWaterMark(maxReceivedWatermark[0], tagEvents);
        ProcessStreamWaterMark(maxReceivedWatermark[1], likeEvents);

        // make new watermark event
        Event newWaterMarkEvent = Event.CreateEvent(Math.Max(maxReceivedWatermark[0], maxReceivedWatermark[1]), EventType.Watermark, Array.Empty<byte>());
        // emit new watermark event
        await outputStream.OnNextAsync(newWaterMarkEvent);
    }

    private void ProcessStreamWaterMark(long WM, Dictionary<long, List<Event>> events)
    {
        if (WM <= 0) return; // skip processing

        // retreive windows
        List<long> windowIDs = GetRelevantWindowStarts(WM, this.windowSlide, this.windowLength).ToList();
        foreach (long windowStart in windowIDs)
        {
            long windowEnd = windowStart + this.windowLength;
            if (windowEnd <= WM) events.Remove(windowStart);// iff the window is completely before the watermark, we can delete it
        }
    }

    async Task ProcessRegularEvent(Event e, int sourceID)
    {

        // if (sourceID == 1 && )
        bool didPrint = false;

        // get the windows for each stream
        Dictionary<long, List<Event>> currentStreamEvents = (sourceID == 1) ? tagEvents : likeEvents;
        Dictionary<long, List<Event>> otherStreamEvents = (sourceID == 1) ? likeEvents : tagEvents;

        // TESTING: Extract and print tag or like event details
        Tuple<int, int> eventDetails = Event.GetContent<Tuple<int, int>>(e);
        if (sourceID == 1) // Tag
        {
            Console.WriteLine($"Tag <{e.timestamp}, photoID = {eventDetails.Item1}, userID = {eventDetails.Item2}>");
            didPrint = true;
        }
        else // Like
        {
            Console.WriteLine($"Like <{e.timestamp}, userID = {eventDetails.Item1}, photoID = {eventDetails.Item2}>");
            didPrint = true;
        }
        // Get relevant window starts for the event
        var windowIDs = GetRelevantWindowStarts(e.timestamp, this.windowSlide, this.windowLength);

        // assert that there is at least one relevant window
        Debug.Assert(windowIDs.Count() > 0);

        // TESTING: Print the relevant window starts.
        // foreach (var windowStart in windowStarts)
        // {
        //     Console.WriteLine($"Event timestamp {e.timestamp} falls into window starting at {windowStart}");
        // }



        // go over the window instances
        // Step 1: Store the event in each relevant window
        // Step 2: Attempt to join with events from the other stream in the same window
        foreach (long windowID in windowIDs)
        {
            //store the event
            if (!currentStreamEvents.ContainsKey(windowID))
                currentStreamEvents[windowID] = new List<Event>();
            currentStreamEvents[windowID].Add(e);

            // Determine if there are events in the other stream to join with
            if (otherStreamEvents.ContainsKey(windowID))
            {
                // loop over the events in the other stream
                foreach (Event otherStreamEvent in otherStreamEvents[windowID])
                {
                    // this is possible join candidates
                    // use qeury specific join function to determine if they can be joined
                    // Event joinedEvent = Functions.WindowJoin(windowID + windowLength - 1, e, otherStreamEvent);
                    // we want tag first, like second
                    Event joinedEvent = sourceID == 1 ? Functions.WindowJoin(windowID + windowLength - 1, e, otherStreamEvent)
                                                      : Functions.WindowJoin(windowID + windowLength - 1, otherStreamEvent, e);

                    // if the join is successful, print the joined event
                    if (joinedEvent != null)
                    {
                        var jEve = Event.GetContent<Tuple<long, long, int, int>>(joinedEvent);
                        Console.WriteLine($"Joined: <{joinedEvent.timestamp}, {jEve.Item1}, {jEve.Item2}, photoID = {jEve.Item3}, userID = {jEve.Item4}>");
                        this.joinedCount++;
                        await outputStream.OnNextAsync(joinedEvent);
                    }
                }
            }
        }
        Console.WriteLine($"Printed = {didPrint} => {this.joinedCount} joined events");
        Console.WriteLine("--------------------------------------------------\n\n");
    }

    private IEnumerable<long> GetRelevantWindowStarts(long timestamp, int windowSlide, int windowLength)
    {
        long firstPossibleWindowStart = timestamp - windowLength + windowSlide;
        firstPossibleWindowStart = (firstPossibleWindowStart / windowSlide) * windowSlide; // align to window slide

        long lastPossibleWindowStart = Helper.GetWindowInstanceID(timestamp, windowSlide);

        for (long windowStart = firstPossibleWindowStart; windowStart <= lastPossibleWindowStart; windowStart += windowSlide)
        {
            if (windowStart >= 0) // Avoid negative window starts
                yield return windowStart;
        }
    }
}