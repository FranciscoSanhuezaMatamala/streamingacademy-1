import os
from quixstreams import Application
import uuid
import json

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()

app = Application.Quix(str(uuid.uuid4()), auto_offset_reset="earliest")

input_topic = app.topic(os.environ["input"])
#output_topic = app.topic(os.environ["output"])

sdf = app.dataframe(input_topic)

# put transformation logic here
# see docs for what you can do
# https://quix.io/docs/get-started/quixtour/process-threshold.html

sdf = sdf.apply(lambda message: message["payload"], expand=True)

def transpose(row: dict):

    result = {
        "time": row["time"]
    }

    for key in row["values"]:
        result[row["name"] + "-" + key] = row["values"][key]

    return result
        
sdf = sdf.apply(transpose)

sdf = sdf[sdf.contains("location-latitude")]
sdf = sdf[["time", 'location-latitude', "location-longitude", "location-speed"]]
sdf["location-speed"] = sdf["location-speed"] * 3.6

sdf = sdf.update(lambda row: print(row))

#sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)