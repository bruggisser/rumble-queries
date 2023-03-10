import module namespace hep = "https://raw.githubusercontent.com/bruggisser/rumble-queries/main/queries/v0/common/hep.jq";
import module namespace query-6 = "https://raw.githubusercontent.com/bruggisser/rumble-queries/main/queries/v0/query-6-common/common.jq";

let $filtered :=
  for $event in parquet-file("INPUT_PATH", 54)
  where size($event.Jet) > 1
  return query-6:find-min-triplet($event).trijet.pt

return hep:histogram($filtered, 15, 40, 100)