

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
        "fullDocument.obs.lightning_strike_count_last_1hr": { $lt: 2 },
        "fullDocument.obs.lightning_strike_last_distance": { $lt: 20 }
    }
}

project = {$project: 
    {
        "station_id":"$fullDocument.station_id", 
        "strike_found": "1",
        "strike_last_dist": "$fullDocument.obs.lightning_strike_last_distance",
        "strike_count_last_1hr": "$fullDocument.obs.lightning_strike_count_last_1hr",
        "event_time": "$_ts",
        "_id": 0
    }
}

merge = {$merge: 
    { 
        into: {
            connectionName: "AtlasDBSource",
            db: "KG_demo",
            coll: "LightningStrike"
        },
        on: "station_id", 
        whenMatched: "replace", 
        whenNotMatched: "insert" 
    }
} 

p = [source, unset, unwind, match, project, merge]
sp.createStreamProcessor("LightningStrike", p)
sp.LightningStrike.start()