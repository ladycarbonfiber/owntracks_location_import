Small tool that converts the results of Google Location Data take out (pre ondevice only migration) to the Owntracks format for selfhosting. 

Uses the polars df lib for data manipulation and serde for serializing.

invoke with `location_import -i <file.json> -e <device id to ignore> -i <desired OT device id (two chars)>`

Synethic data created via Claude and spot checked to confirm it's all out in the ocean so (hopefully) not someone's actual data reproduced from the training set