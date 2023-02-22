import module namespace hep = "https://raw.githubusercontent.com/bruggisser/rumble-queries/main/queries/v0/common/hep.jq";

let $filtered := parquet-file("INPUT_PATH").Jet[].pt

return hep:histogram($filtered, 15, 60, 100)
