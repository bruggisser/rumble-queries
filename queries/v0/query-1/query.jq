import module namespace hep = "https://raw.githubusercontent.com/bruggisser/rumble-queries/main/queries/v0/common/hep.jq";

let $filtered := parquet-file("INPUT_PATH").MET.pt
return hep:histogram($filtered, 0, 2000, 100)
