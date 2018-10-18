package jobOneHour.subJob.dnsCovDetect

case class DnsFlow(
                  srcIp: String,
                  destIp: String,
                  srcPort: Double,
                  destPort: Double,
                  proto: Int,
                  flagsResponse: Int,
                  flagsTruncated: Int,
                  size: Int,
                  answerRRS: Int,
                  questionRRS: Int,
                  authorityRRS: Int,
                  additionalRRS: Int,
                  queryType: Int,
                  queryLen: Int,
                  queryEnt: Double,
                  queryNumRatio: Double,
                  queryLabelCount: Int,
                  maxAnsTTL: Int,
                  answersAddr: Int,
                  maxAnsTXT: Int,
                  maxAnsData: Int,
                  sumAnsTXT: Int,
                  sumAnsData: Int
                   )
