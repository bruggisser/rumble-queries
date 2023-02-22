import module namespace hep = "https://raw.githubusercontent.com/bruggisser/rumble-queries/main/queries/v1/common/hep.jq";

let $filtered :=
  for $jet in parquet-file("INPUT_PATH").Jet[]
  where abs($jet.eta) < 1
  return $jet.pt

return hep:histogram($filtered, 15, 60, 100)
