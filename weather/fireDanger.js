source = {$source:
   {
      "connectionName":"AtlasDBSource", 
      "db": "datafeeds", 
      "coll": "kg_weather"
   }
}

unset = {$unset: "fullDocument.outdoor_keys"}
unwind = {$unwind: {path: "$fullDocument.obs"}}

match = {$match: 
  {
      "obs.relative_humidity": { $lte: 99 },
      "obs.wind_gust": { $gte: 0 } 
  }
}

window = {$tumblingWindow: 
   {
      interval: {
         size: NumberInt(3),
         unit: "minute"
         },
      pipeline: [
         {$group: {
            _id: "$fullDocument",
            max_wind_gust: {
               $max: "$fullDocument.obs.wind_gust"
            },
            avg_wind_gust: {
               $avg: "$fullDocument.obs.wind_gust"
            }
         }
         }
      ]
   }
}

merge = {$merge: 
    {
        into: {
            connectionName: "AtlasDBSource",
            db: "KG_demo",
            coll: "fire_danger"
        }
    }

}

processor = [source, match, merge]
sp.process(processor)


window = {$tumblingWindow: 
   {
      interval: {
         size: NumberInt(1),
         unit: "minute"
         },
      pipeline: [
         {$unwind: {path: "$obs"}},
         {$group: {
            _id: "$station_id",
            max: {
               $max: "$obs.wind_gust"
            },
            avg: {
               $avg: "$obs.wind_gust"
            }
         }
         }
      ]
   }
}





