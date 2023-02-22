import module namespace hep = "https://raw.githubusercontent.com/bruggisser/rumble-queries/main/queries/v1/common/hep.jq";
import module namespace query-6 = "https://raw.githubusercontent.com/bruggisser/rumble-queries/main/queries/v1/query-6-common/common.jq";

let $filtered :=
  for $event in parquet-file("INPUT_PATH")
  where size($event.Jet) > 2
  let $min-triplet := query-6:find-min-triplet($event)
  return max($min-triplet.jets[].btag)

return hep:histogram($filtered, 0, 1, 100)