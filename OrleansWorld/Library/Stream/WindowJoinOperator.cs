﻿using Orleans.Streams;
using Utilities;
using SocialNetwork;
using System.Diagnostics;

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
                ProcessRegularEvent(e, 1);
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
                ProcessRegularEvent(e, 2);
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
        throw new NotImplementedException();
    }

    async Task ProcessRegularEvent(Event e, int sourceID)
    {
        Dictionary<long, List<Event>> currentStreamEvents = (sourceID == 1) ? tagEvents : likeEvents;
        Dictionary<long, List<Event>> otherStreamEvents = (sourceID == 1) ? likeEvents : tagEvents;

        // Calculate the start window ID and add two more windows to cover the overlap
        long startWindowId = Helper.GetWindowInstanceID(e.timestamp, windowSlide);
        List<long> relevantWindows = new List<long>
        {
            startWindowId,
            startWindowId + windowSlide,
            startWindowId + 2 * windowSlide
        };

        // Extract and print tag or like event details
        Tuple<int, int> eventDetails = Event.GetContent<Tuple<int, int>>(e);
        if (sourceID == 1) // Tag
        {
            Console.WriteLine($"Tag:ts = {e.timestamp}, photoID = {eventDetails.Item1}, userID = {eventDetails.Item2}. Window1 = {relevantWindows[0]}, Window2 = {relevantWindows[1]}");
        }
        else // Like
        {
            Console.WriteLine($"Like: ts = {e.timestamp}, userID = {eventDetails.Item1}, photoID = {eventDetails.Item2}. Window1 = {relevantWindows[0]}, Window2 = {relevantWindows[1]}");
        }

        // Store the event in each relevant window and attempt to join with events from the other stream
        foreach (var windowId in relevantWindows)
        {
            if (!currentStreamEvents.ContainsKey(windowId))
                currentStreamEvents[windowId] = new List<Event>();
            currentStreamEvents[windowId].Add(e);

            if (otherStreamEvents.ContainsKey(windowId))
            {
                foreach (Event otherStreamEvent in otherStreamEvents[windowId])
                {
                    long upperBoundaryTimestamp = windowId + windowLength - 1;
                    Event joinedEvent = Functions.WindowJoin(upperBoundaryTimestamp, e, otherStreamEvent);

                    if (joinedEvent != null)
                    {
                        var jEve = Event.GetContent<Tuple<long, long, int, int>>(joinedEvent);
                        Console.WriteLine($"Joined: ts = {joinedEvent.timestamp}, {jEve.Item1}, {jEve.Item2}, photoID = {jEve.Item3}, userID = {jEve.Item4}");
                        await outputStream.OnNextAsync(joinedEvent);
                    }
                }
            }
        }
    }


    // async Task ProcessRegularEvent(Event e, int sourceID)
    // {

    //     // TAGS:  <timestamp, photoID, userID>
    //     // LIKES: <timestamp, userID, photoID>

    //     Dictionary<long, List<Event>> currentStreamEvents = (sourceID == 1) ? tagEvents : likeEvents;
    //     Dictionary<long, List<Event>> otherStreamEvents = (sourceID == 1) ? likeEvents : tagEvents;

    //     // Get relevant windows
    //     List<long> relevantWindows = new List<long>
    //     {
    //         Helper.GetWindowInstanceID(e.timestamp, windowSlide),
    //         Helper.GetWindowInstanceID(e.timestamp, windowSlide) + windowLength - windowSlide
    //     };

    //     // Extract and print tag or like event details
    //     Tuple<int, int> eventDetails = Event.GetContent<Tuple<int, int>>(e);
    //     if (sourceID == 1) // Tag
    //     {
    //         Console.WriteLine($"Tag:ts = {e.timestamp}, photoID = {eventDetails.Item1}, userID = {eventDetails.Item2}. Window1 = {relevantWindows[0]}, Window2 = {relevantWindows[1]}");
    //     }
    //     else // Like
    //     {
    //         Console.WriteLine($"Like: ts = {e.timestamp}, userID = {eventDetails.Item2}, photoID = {eventDetails.Item1}. Window1 = {relevantWindows[0]}, Window2 = {relevantWindows[1]}");
    //     }

    //     // Store the event in each relevant window and attempt to join with events from the other stream
    //     foreach (var windowId in relevantWindows)
    //     {
    //         if (!currentStreamEvents.ContainsKey(windowId))
    //             currentStreamEvents[windowId] = new List<Event>();
    //         currentStreamEvents[windowId].Add(e);

    //         if (otherStreamEvents.ContainsKey(windowId))
    //         {
    //             foreach (Event otherStreamEvent in otherStreamEvents[windowId])
    //             {
    //                 long upperBoundaryTimestamp = windowId + windowLength - 1;
    //                 Event joinedEvent = Functions.WindowJoin(upperBoundaryTimestamp, e, otherStreamEvent);

    //                 if (joinedEvent != null)
    //                 {
    //                     var jEve = Event.GetContent<Tuple<long, long, int, int>>(joinedEvent);
    //                     Console.WriteLine($"Joined: ts = {joinedEvent.timestamp}, {jEve.Item1}, {jEve.Item2}, photoID = {jEve.Item3}, userID = {jEve.Item4}");
    //                     await outputStream.OnNextAsync(joinedEvent);
    //                 }
    //             }
    //         }
    //     }
    // }
}
