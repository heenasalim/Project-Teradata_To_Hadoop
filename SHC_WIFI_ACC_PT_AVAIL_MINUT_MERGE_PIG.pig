%default CURRENT_DATE `date +%Y%m%d`;
%declare CURRENT_TIMESTAMP `date "+'%Y-%m-%d %H:%m:%S'"`


ACCESS_PT_AVAIL_MINUT_Target  = LOAD '/incoming/netwatch/availability/SHC_WIFI_ACCESS_PT_AVAIL_MINUT_TABLE_20170413/' USING PigStorage(',') AS 
    ( 
    ACCESS_PT_ID:CHARARRAY,
    ACCESS_PT_STAT_DT:CHARARRAY,
    LOCN_NBR:INT,
    ACCESS_PT_MINUT_ID:INT,
    ACCESS_PT_UNAVL_IND:CHARARRAY,
    DATASET_ID:INT,
    CREAT_TS:CHARARRAY,
    MOD_TS:CHARARRAY);

MINUTE_A = LOAD '/user/hive/warehouse/minut_a' USING PigStorage(',') AS
    (
     ACCESS_PT_ID:CHARARRAY,
     ACCESS_PT_STAT_DT:CHARARRAY,
     LOCN_NBR:INT,
     ACCESS_PT_MINUT_ID:INT,
     ACCESS_PT_UNAVL_IND:CHARARRAY,
     DATASET_ID:INT,
     CREAT_TS:CHARARRAY,
     MOD_TS:CHARARRAY);


LEFT_JOIN = JOIN ACCESS_PT_AVAIL_MINUT_Target BY (ACCESS_PT_ID ,ACCESS_PT_STAT_DT ,LOCN_NBR ,ACCESS_PT_MINUT_ID ) left outer,MINUTE_A BY (ACCESS_PT_ID ,ACCESS_PT_STAT_DT ,LOCN_NBR ,ACCESS_PT_MINUT_ID );

RIGHT_JOIN = JOIN ACCESS_PT_AVAIL_MINUT_Target BY (ACCESS_PT_ID ,ACCESS_PT_STAT_DT ,LOCN_NBR ,ACCESS_PT_MINUT_ID )  right outer,MINUTE_A  BY (ACCESS_PT_ID ,ACCESS_PT_STAT_DT ,LOCN_NBR ,ACCESS_PT_MINUT_ID ) ;


a = FOREACH LEFT_JOIN GENERATE  ACCESS_PT_AVAIL_MINUT_Target::ACCESS_PT_ID,
                                ACCESS_PT_AVAIL_MINUT_Target::ACCESS_PT_STAT_DT,
                                ACCESS_PT_AVAIL_MINUT_Target::LOCN_NBR,
                                ACCESS_PT_AVAIL_MINUT_Target::ACCESS_PT_MINUT_ID,
                                (MINUTE_A::ACCESS_PT_ID IS NULL ? ACCESS_PT_AVAIL_MINUT_Target::ACCESS_PT_UNAVL_IND  : MINUTE_A::ACCESS_PT_UNAVL_IND),
                                ACCESS_PT_AVAIL_MINUT_Target::DATASET_ID,
                                ACCESS_PT_AVAIL_MINUT_Target::CREAT_TS,
                                (MINUTE_A::ACCESS_PT_ID IS NULL ? ACCESS_PT_AVAIL_MINUT_Target::MOD_TS               : $CURRENT_TIMESTAMP);


INSERT_VALUES = filter RIGHT_JOIN  BY ACCESS_PT_AVAIL_MINUT_Target::ACCESS_PT_ID is null;


INSERT_NEW = foreach INSERT_VALUES  generate   MINUTE_A::ACCESS_PT_ID,
                                               MINUTE_A::ACCESS_PT_STAT_DT,
                                               MINUTE_A::LOCN_NBR,
                                               MINUTE_A::ACCESS_PT_MINUT_ID,
                                               MINUTE_A::ACCESS_PT_UNAVL_IND,
                                               MINUTE_A::DATASET_ID,                                                                                                                                                   MINUTE_A::CREAT_TS,        
                                               MINUTE_A::MOD_TS;

UNION_DATA = UNION INSERT_NEW,a;

STORE UNION_DATA into '/incoming/netwatch/availability/2017_04_18_april';

