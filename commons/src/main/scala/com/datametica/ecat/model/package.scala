package com.datametica.ecat

/**
  *This module consist of entities to be used by ecat for authentication and lineage details
 */
package object model {

  case class LineageDetails(
                             jobName: String,
                             jobType: String,
                             executionEngineType: String,
                             dataStoreName: String,
                             batchId: String,
                             lineageOperationList: List[LineageOperation]
                           )

  case class LineageSourceDetails(fqname: String)

  case class LineageOperation(
                               lineageSource: LineageSourceDetails,
                               lineageDestination: LineageSourceDetails,
                               jobOwner: String,
                               jobId: String,
                               jobName: String
                             )

  case class Token(
                    access_token: String,
                    token_type: String,
                    refresh_token: String
                  )

}
