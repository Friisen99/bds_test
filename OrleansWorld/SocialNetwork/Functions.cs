using Confluent.Kafka;
using Utilities;

namespace SocialNetwork;

public static class Functions
{
    public static Func<Event, bool> FilterExample = e =>
    {
        var number = BitConverter.ToInt32(e.content);
        return number < 50;
    };

    public static Func<long, Event, Event, Event> WindowJoin = (timestamp, e1, e2) =>
    {
        // extract tag and like events details
        var tagEvent  = Event.GetContent<Tuple<int, int>>(e1); // Tuple<photoID, userID>
        var likeEvent = Event.GetContent<Tuple<int, int>>(e2); // Tuple<userID,  photoID>

        //check if photoID and userID match
        if (tagEvent.Item1 == likeEvent.Item2 && tagEvent.Item2 == likeEvent.Item1)
        {
            // Create a new event with the format <ts: photoID, userID>
            return Event.CreateEvent(timestamp, EventType.Regular, 
                new Tuple<long, long, int, int>(e1.timestamp, e2.timestamp, tagEvent.Item1, tagEvent.Item2));
        }
        else
        {
            return null; // Return null if the conditions for joining are not met
        }
    };

    public static Func<Event, bool> Filter = e =>
    {
        // <ts: photo tagged the user, ts: user liked the photo, photo ID, user ID>
        var joinedResult = Event.GetContent<Tuple<long, long, int, int>>(e);
        return joinedResult.Item1 < joinedResult.Item2;   // user likes the photo after he/she is tagged
    };

    public static Func<long, List<Event>, List<Event>> WindowAggregator = (timestamp, events) =>
    {
        //initialize a dictionary to store the number of likes for each photo
        var photoLikes = new Dictionary<int, MyCounter>();
        //go over the events in the window and aggregate the number of likes for each photo
        foreach (var e in events)
        {
            var joinedResult = Event.GetContent<Tuple<long, long, int, int>>(e);
            if (!photoLikes.ContainsKey(joinedResult.Item3))
                photoLikes[joinedResult.Item3] = new MyCounter();
            photoLikes[joinedResult.Item3].Increment();
        }
        //create a list of events to return
        var result = new List<Event>();
        foreach (var photoID in photoLikes.Keys)
        {
            result.Add(Event.CreateEvent(timestamp, EventType.Regular, new Tuple<int, int>(photoID, photoLikes[photoID].Get())));
        }
        return result;
    };

    public static Func<string, Event, Null> Sink = (resultFile, e) =>
    {
        if (e.type == EventType.Regular)
        {
            using (var file = new StreamWriter(resultFile, true))
            {
                var content = Event.GetContent<Tuple<int, int>>(e);
                Console.WriteLine($"output: ts = {e.timestamp}, photoID = {content.Item1}, count = {content.Item2}");
                file.WriteLine($"{content.Item1} {content.Item2}");
            }
        }
        return null;
    };
}

internal class MyCounter
{
    int n;

    public MyCounter() => n = 0;

    public void Increment() => n++;

    public int Get() => n;    
}