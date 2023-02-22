import module namespace hep = "https://raw.githubusercontent.com/bruggisser/rumble-queries/main/queries/v1/common/hep.jq";

let $filtered := (
  for $event at $q in parquet-file("INPUT_PATH")
  where size($event.Muon) + size($event.Electron) > 2

  let $leptons := hep:concat-leptons($event)
  let $closest-lepton-pair := (
    for $lepton1 at $i in $leptons
    for $lepton2 at $j in $leptons
    where $i < $j
    where $lepton1.type eq $lepton2.type and $lepton1.charge ne $lepton2.charge
    order by abs(91.2 - hep:add-PtEtaPhiM($lepton1, $lepton2).mass) ascending
    return {"i": $i, "j": $j}
  )[1]
  where exists($closest-lepton-pair)

  let $other-leption := (
    for $lepton at $i in $leptons
    where $i ne $closest-lepton-pair.i and $i ne $closest-lepton-pair.j
    order by $lepton.pt descending
    return $lepton
  )[1]

  return float(sqrt(float(2) * $event.MET.pt * $other-leption.pt *
    (1.0 - float(cos(hep:delta-phi($event.MET.phi, $other-leption.phi))))))
)

return hep:histogram($filtered, 15, 250, 100)
