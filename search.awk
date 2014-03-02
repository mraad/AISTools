BEGIN{
  FS="\t"
  OFS=","
  print "ID","LON","LAT","DATETIME","VOYAGEID","MMSI"
}
{
  print $1,$2,$3,"\""$8"\"",$10,$11
}
