import module namespace hep = "https://raw.githubusercontent.com/bruggisser/rumble-queries/main/queries/v1/common/hep.jq";

let $filtered :=
  for $event in parquet-file("INPUT_PATH")
  where count($event.Jet[][$$.pt > 40]) > 1
  return $event.MET.pt

return hep:histogram($filtered, 0, 2000, 100)