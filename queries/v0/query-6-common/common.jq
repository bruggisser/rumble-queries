module namespace query-6 = "https://raw.githubusercontent.com/bruggisser/rumble-queries/main/queries/v0/query-6-common/common.jq";
import module namespace hep = "https://raw.githubusercontent.com/bruggisser/rumble-queries/main/queries/v0/common/hep.jq";

declare function query-6:find-min-triplet($event) {
  (
    for $jet1 at $i in $event.Jet[]
    for $jet2 at $j in $event.Jet[]
    for $jet3 at $k in $event.Jet[]
    where $i < $j and $j < $k
    let $tri-jet := hep:make-tri-jet($jet1, $jet2, $jet3)
    order by abs(172.5 - $tri-jet.mass) ascending
    return {"trijet": $tri-jet, "jets": [$jet1, $jet2, $jet3]}
  )[1]
};
