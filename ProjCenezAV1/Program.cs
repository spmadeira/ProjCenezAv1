using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using ProjCenezAV1.MapReduce;

namespace ProjCenezAV1
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var text = string.Join("\n", Enumerable.Repeat("Deer Bear River\nCar Car River\nDeer Car Bear", 500));

            var builder = MapReduce.MapReduce
                .WithInput<string>()
                .WithReader(input => input.Split("\n"))
                .WithMapper(data =>
                {
                    var keys = data.Split(" ");
                    return keys.Select(k => new KeyValuePair<string, int>(k, 1));
                })
                .WithReducer((word, instances) => instances.Sum())
                .WithWriter(pair => System.Console.WriteLine($"Key: {pair.Key} | Value: {pair.Value}"));
                
            var llmr = builder.Build<LLMapReduce<string, string, string, int>>();

            var sw = Stopwatch.StartNew();
            await llmr.RunAsync(text, 4);
            sw.Stop();

            Console.WriteLine($"Low-Level Map Reduce ran in {sw.ElapsedMilliseconds}ms");
            
            //Ex. Output
            //Key: Deer | Value: 1000
            //Key: Bear | Value: 1000
            //Key: River | Value: 1000
            //Key: Car | Value: 1500
            //Low-Level Map Reduce ran in 76ms
        }
    }
}
