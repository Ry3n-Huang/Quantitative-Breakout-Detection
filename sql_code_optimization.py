from sqlalchemy import create_engine

db_url = 'database_info'
engine = create_engine(db_url)

query = f'''select distinct  swindex.S_INFO_WINDCODE,swindex.S_CON_WINDCODE,swindex.S_CON_INDATE,swindex.S_CON_OUTDATE,sector.S_INFO_NAME from SWINDEXMEMBERS
 AS swindex,ASHARESWNINDUSTRIESCLASS AS swindustry,INDEXCONTRASTSECTOR AS sector,ASHAREINDUSTRIESCODE as indcode  WHERE swindex.S_INFO_WINDCODE=sector.S_INFO_INDEXCODE AND sector.S_INFO_INDUSTRYCODE=indcode.INDUSTRIESCODE 
AND LEFT(swindustry.SW_IND_CODE,4)=LEFT(indcode.INDUSTRIESCODE,4) AND indcode.LEVELNUM=2  AND swindex.S_CON_WINDCODE in ('002468.SZ', '600519.SH')'''

print(query)