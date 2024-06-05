--##############################################################################
--#                              General Details                               #
--##############################################################################
--#                                                                            #
--# Name                                                                       #
--#     : deduplication                                                        #
--# File                                                                       #
--#     : module-transform.pig                                                 #
--# Description                                                                #
--#     : dedups records based on md5 of specified columns                     #
--#                                                                            #
--#                                                                            #
--#                                                                            #
--# Author                                                                     #
--#     : Hemanth Meka                                                         #
--#                                                                            #
--##############################################################################
--#                                                                            #
--##############################################################################


REGISTER '${PIG_UDF_BANK}/util.jar';

DEFINE COALESCECONCATANDMD5 'com.datametica.hadoop.pig.util.CoalesceConcatAndMd5';
DEFINE MARKBAGTILLPOSITION  'com.datametica.hadoop.pig.util.MarkBagTillPosition' ;

--##############################################################################
--#                                   LOAD                                     #
--##############################################################################

load_inp = LOAD '${HIVE_DATABASE_PREFIX}${HIVE_DATABASE_NAME_WORK}.${HIVE_TABLE_PREFIX}${INPUT_TABLE_NAME}'      USING org.apache.hive.hcatalog.pig.HCatLoader();

md5      = FOREACH load_inp GENERATE 
                                      *, 
                                      COALESCECONCATANDMD5($COLUMNS_TO_GENERATE_MD5) AS md5;

grp      = GROUP md5 BY md5;

mark     = FOREACH grp      GENERATE 
                                      FLATTEN(MARKBAGTILLPOSITION(1, md5));

SPLIT mark INTO 
                originals  IF mark == true, 
                duplicates IF mark != true;

--##############################################################################
--#                                   Store                                    #
--##############################################################################

STORE originals INTO '${HIVE_DATABASE_PREFIX}${HIVE_DATABASE_NAME_WORK}.${HIVE_TABLE_PREFIX}${OUTPUT_TABLE_NAME}' USING org.apache.hive.hcatalog.pig.HCatStorer();

STORE originals INTO '${HIVE_DATABASE_PREFIX}${HIVE_DATABASE_NAME_WORK}.${HIVE_TABLE_PREFIX}${ERROR_TABLE_NAME}'  USING org.apache.hive.hcatalog.pig.HCatStorer();

--##############################################################################
--#                                    End                                     #
--##############################################################################
