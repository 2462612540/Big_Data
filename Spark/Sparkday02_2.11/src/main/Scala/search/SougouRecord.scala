package search

/**
 * 用户的搜索点击网页记录的Record
 *
 * @param queryTime  用户的访问时间
 * @param userId     用户的ID
 * @param queryWords 查询的词
 * @param resultRank 该URL在返回结果的排名
 * @param clickRank  用户的点击的顺序号
 * @param clickUrl   用户的点击的URL
 */
case class SougouRecord(
                         queryTime: String,
                         userId: String,
                         queryWords: String,
                         resultRank: Int,
                         clickRank: Int,
                         clickUrl: String
                       )
