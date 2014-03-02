BEGIN{
  FS="\t"
  OFS=","
  print "ORIGID","POPULATION"
}
{
  print $1,$2
}
