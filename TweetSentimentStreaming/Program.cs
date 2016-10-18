using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Diagnostics;
using Tweetinvi;

namespace TweetSentimentStreaming
{
    class Program
    {
        const string TWITTERAPPACCESSTOKEN = "3012558578-k22IauL3UCRl6iB7206JOucDYxXotUZDsnFbLdg";
        const string TWITTERAPPACCESSTOKENSECRET = "LnPXijkOjp5XRRwAoGGexoBbg8jelcJUYoXbU6OCTlYU9";
        const string TWITTERAPPAPIKEY = "8ArzsPmF9aiJoICthtTSmqpdC";
        const string TWITTERAPPAPISECRET = "WuTjO5yvsz6xOiPb1x8A9D8bYqoEMmkfV0T0iAQ1wr1b9omiOd";

        static void Main(string[] args)
        {
            Console.Write(" *** Started ***");
            TwitterCredentials.SetCredentials(TWITTERAPPACCESSTOKEN, TWITTERAPPACCESSTOKENSECRET, TWITTERAPPAPIKEY, TWITTERAPPAPISECRET);
            Stream_FilteredStreamExample();
        }


        private static void Stream_FilteredStreamExample()
        {
            for (; ; )
            {
                try
                {
                    HBaseWriter hbase = new HBaseWriter();
                    var stream = Stream.CreateFilteredStream();
                    stream.AddLocation(Geo.GenerateLocation(-180, -90, 180, 90));

                    var tweetCount = 0;
                    var timer = Stopwatch.StartNew();

                    stream.MatchingTweetReceived += (sender, args) =>
                    {
                        tweetCount++;
                        var tweet = args.Tweet;

                        // Write Tweets to HBase
                        hbase.WriteTweet(tweet);

                        if (timer.ElapsedMilliseconds > 1000)
                        {
                            if (tweet.Coordinates != null)
                            {
                                Console.ForegroundColor = ConsoleColor.Green;
                                Console.WriteLine("\n{0}: {1} {2}", tweet.Id, tweet.Language.ToString(), tweet.Text);
                                Console.ForegroundColor = ConsoleColor.White;
                                Console.WriteLine("\tLocation: {0}, {1}", tweet.Coordinates.Longitude, tweet.Coordinates.Latitude);
                            }

                            timer.Restart();
                            Console.WriteLine("\tTweets/sec: {0}", tweetCount);
                            tweetCount = 0;
                        }
                    };

                    stream.StartStreamMatchingAllConditions();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Exception: {0}", ex.Message);
                }
            }
        }
    }
}