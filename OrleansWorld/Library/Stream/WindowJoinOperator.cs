using Orleans.Streams;
using Utilities;
using SocialNetwork;
using System.Diagnostics;
using System.Net.WebSockets;
using System.Linq;

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
    private Dictionary<long, List<Event>> tagEvents;
    private Dictionary<long, List<Event>> likeEvents;

    public async Task Init(IAsyncStream<Event> inputStream1, IAsyncStream<Event> inputStream2, IAsyncStream<Event> outputStream, int windowSlide, int windowLength)
    {
        this.windowSlide = windowSlide;
        this.windowLength = windowLength;
        Debug.Assert(windowLength % windowSlide == 0);
        this.outputStream = outputStream;

        this.tagEvents = new Dictionary<long, List<Event>>();
        this.likeEvents = new Dictionary<long, List<Event>>();
        
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
                await ProcessWatermark(1);
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
                await ProcessWatermark(2);
                break;
            default:
                throw new Exception($"Exception event type {e.type} is not supported. ");
        }
    }

    async Task ProcessWatermark(int sourceID)
    {
        // get the WM
        long WM = maxReceivedWatermark[sourceID - 1];

        // get the windows for each stream
        Dictionary<long, List<Event>> otherStreamEvents = (sourceID == 1) ? likeEvents : tagEvents;

        // ts < wm - windowLength should be removed
        var numberOfElementsRemoved = otherStreamEvents.Where(ev => ev.Key < WM).Count();
        otherStreamEvents = otherStreamEvents.Where(ev => ev.Key >= WM)
            .ToDictionary(t => t.Key, t => t.Value);

        //emit new event
        Event newWaterMarkEvent = Event.CreateEvent(Math.Min(maxReceivedWatermark[0], maxReceivedWatermark[1]), EventType.Watermark, Array.Empty<byte>());
        await outputStream.OnNextAsync(newWaterMarkEvent);
    }

    async Task ProcessRegularEvent(Event e, int sourceID)
    {
        // get the windows for each stream
        Dictionary<long, List<Event>> currentStreamEvents = (sourceID == 1) ? tagEvents : likeEvents;
        Dictionary<long, List<Event>> otherStreamEvents = (sourceID == 1) ? likeEvents : tagEvents;

        // Get relevant window starts for the event
        var windowIDs = GetRelevantWindowStarts(e.timestamp, this.windowSlide, this.windowLength);


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
                    // we want tag first, like second
                    Event joinedEvent = sourceID == 1 ? Functions.WindowJoin(windowID + windowLength - 1, e, otherStreamEvent)
                                                      : Functions.WindowJoin(windowID + windowLength - 1, otherStreamEvent, e);

                    // if the join is successful, emit the new joined event
                    if (joinedEvent != null) await outputStream.OnNextAsync(joinedEvent);
                }
            }
        }
    }

    private IEnumerable<long> GetRelevantWindowStarts(long timestamp, int windowSlide, int windowLength)
    {
        long firstPossibleWindowStart = timestamp - (windowLength - windowSlide) - (timestamp % windowSlide);
        long lastPossibleWindowStart = Helper.GetWindowInstanceID(timestamp, windowSlide);

        for (long windowStart = firstPossibleWindowStart; windowStart <= lastPossibleWindowStart; windowStart += windowSlide)
        {
            yield return windowStart;
        }
    }
}