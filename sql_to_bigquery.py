import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam import PTransform, DoFn, ParDo
import pyodbc
import argparse
from datetime import datetime
import logging
from google.cloud import bigquery

bigquery_client = bigquery.Client()

class MssqlSincroHelper:
    def __init__(self,host, port, user, password, database, table, schema):
        self.database = database
        self.table = table
        self.host = host
        self.port = port
        self.schema = schema
        self.user = user
        self.password = password
        sql = f"""SELECT c.name FROM [{self.database}].sys.tables t inner join sys.columns c on t.object_id = c.object_id
                    inner join sys.types ty on c.system_type_id = ty.system_type_id and c.user_type_id = ty.user_type_id
        WHERE t.name = '{self.table}'
        """
        cursor = self.get_cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        self.columns = []
        self.columns_changes = []
        self.columns_changes.append("__$operation")
        for c in result:
            self.columns.append(c[0])
            self.columns_changes.append(c[0])
        cursor.close()
        self.columns_changes.append("change_datetime")
        self.columns_changes.append("normalized_time")
        sql = f"""
        SELECT c.name FROM [{self.database}].sys.tables t 
                    inner join [{self.database}].sys.columns c on t.object_id = c.object_id
                    inner join [{self.database}].sys.indexes i on t.object_id = i.object_id
                    inner join [{self.database}].sys.index_columns ci on t.object_id = ci.object_id and i.index_id = ci.index_id and ci.column_id =c.column_id
                WHERE t.name = '{self.table}'
                and i.is_primary_key = 1
        """
        cursor = self.get_cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        self.pk_columns = []
        self.pk_columns_coma_delimited = ""
        coma=""
        for c in result:
            self.pk_columns.append(c[0])
            self.pk_columns_coma_delimited += f"{coma}[{c[0]}]" 
            coma = ", "
        cursor.close()
    
    def cols_sin_pk(self):
        return [item for item in self.columns if item not in self.pk_columns]

    def cols_coma_delimited(self):
        cols = ""
        coma = ""
        for c in self.columns:
            cols += coma + c
            coma= ", "
        return cols

    def cols_changes_coma_delimited(self):
        cols = ""
        coma = ""
        for c in self.columns_changes:
            cols += coma + c
            coma= ", "
        return cols

    def get_cursor(self): 
        conn = pyodbc.connect("DRIVER={ODBC Driver 18 for SQL Server}" + f";SERVER={self.host},{self.port};DATABASE={self.database};UID={self.user};PWD={self.password};Encrypt=no")
        conn.autocommit = True
        return conn.cursor()
    
    def get_value(self, sql, params = None):
        cursor = self.get_cursor()
        if params:
            result = cursor.execute(sql, params)     
        else:
            result = cursor.execute(sql)
        row = result.fetchone()
        if row is None:
            return None
        else:
            return row[0]

class GetMssqlChanges(DoFn):
    def __init__(self, mssqlSincroHelper, last_synced_lsn, last_unsynced_lsn, full_refresh ):
        self.mssqlSincroHelper = mssqlSincroHelper
        self.last_synced_lsn = last_synced_lsn
        self.last_unsynced_lsn = last_unsynced_lsn
        self.full_refresh = full_refresh
        if full_refresh:
            self.sql = f"SELECT * FROM [{self.mssqlSincroHelper.database}].[{self.mssqlSincroHelper.schema}].[{self.mssqlSincroHelper.table}]"
        else:
            select_cols = self.mssqlSincroHelper.cols_changes_coma_delimited().replace("__$operation","__$operation cdc_operation")
            self.sql = f"""
    SET NOCOUNT ON

    DECLARE @P0 varbinary(10) = {self.last_synced_lsn}
    DECLARE @P1 varbinary(10) = {self.last_unsynced_lsn}
    SELECT {select_cols} FROM (
    SELECT *
            , [{self.mssqlSincroHelper.database}].sys.fn_cdc_map_lsn_to_time([__$start_lsn]) change_datetime
            ,row_number() over (partition by {self.mssqlSincroHelper.pk_columns_coma_delimited} ORDER BY [__$start_lsn] DESC, [__$seqval] DESC ) rn
            , cast(null as datetime)  normalized_time
            FROM [{self.mssqlSincroHelper.database}].cdc.[fn_cdc_get_all_changes_dbo_{self.mssqlSincroHelper.table}](@P0, @P1, N'all')
    ) t WHERE rn = 1

        """


    def process(self, element, *args, **kwargs):
        cursor = self.mssqlSincroHelper.get_cursor()
        result = cursor.execute(self.sql)
        rows = result.fetchall()
        for row in rows:
            json = {}

            for i in range(0,len(cursor.description)):
                value = None
                if isinstance(row[i],datetime):
                    value = row[i].strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
                else:
                    value = row[i]
                json[cursor.description[i][0]] = value

            yield json



def get_last_sync_info(sync_config_table, mssqlSincroHelper, destination):
    sql = f"""
            SET NOCOUNT ON
            CREATE TABLE #tmp(last_lsn_synced varbinary(10), last_lsn_unsynced varbinary(10)) 

            update {sync_config_table} SET last_sync_start = GETDATE(),  last_sync_end = NULL
            Output inserted.last_lsn_synced, null into #tmp
            WHERE database_name='{mssqlSincroHelper.database}' AND table_name = '{mssqlSincroHelper.table}' and destination = '{destination}' 
			AND Active = 1 AND last_sync_end IS NOT NULL 

			UPDATE #tmp SET last_lsn_unsynced = [{mssqlSincroHelper.database}].sys.fn_cdc_get_max_lsn()

            SELECT CONVERT(VARCHAR(MAX), last_lsn_synced, 1) last_lsn_synced, CONVERT(VARCHAR(MAX), last_lsn_unsynced, 1) last_lsn_unsynced FROM #tmp
            DROP TABLE #tmp
            """
    cursor = mssqlSincroHelper.get_cursor()
    result = cursor.execute(sql)
    rows = result.fetchall()
    if len(rows) == 1:
        return rows[0][0], rows[0][1]
    return None, None

def get_last_lsn(sync_config_table, mssqlSincroHelper, destination):
    sql = f"""
			SELECT CONVERT(VARCHAR(MAX), [{mssqlSincroHelper.database}].sys.fn_cdc_get_max_lsn(), 1)
            """
    cursor = mssqlSincroHelper.get_cursor()
    result = cursor.execute(sql)
    rows = result.fetchall()
    if len(rows) == 1:
        return rows[0][0]
    return "NULL"

def need_full_refresh(sync_config_table, mssqlSincroHelper, destination):
    sql = f"""
            SELECT last_lsn_synced FROM {sync_config_table}
            WHERE database_name='{mssqlSincroHelper.database}' AND table_name = '{mssqlSincroHelper.table}' and destination = '{destination}' 
			AND Active = 1 
            """
    cursor = mssqlSincroHelper.get_cursor()
    result = cursor.execute(sql)
    rows = result.fetchall()
    if len(rows) == 1:
        return rows[0][0] == None
    return False

def get_bigquery_schema(mssqlSincroHelper):
    sql = f"""
        SET NOCOUNT ON
        DECLARE c CURSOR FOR
        SELECT c.name, CASE c.system_type_id 
				WHEN 34 THEN 'BYTES' -- image
				WHEN 35 THEN 'STRING' -- text
				WHEN 36 THEN 'STRING' -- uniqueidentifier
				WHEN 40 THEN 'DATE' -- date
				WHEN 41 THEN 'TIME' -- time
				WHEN 42 THEN 'DATETIME' -- datetime2
				WHEN 43 THEN 'DATETIME' -- datetimeoffset
				WHEN 48 THEN 'INT64' -- tinyint
				WHEN 52 THEN 'INT64' -- smallint
				WHEN 56 THEN 'INT64' -- int
				WHEN 58 THEN 'DATETIME' -- smalldatetime
				WHEN 59 THEN 'FLOAT64' -- real
				WHEN 60 THEN 'FLOAT64' -- money
				WHEN 61 THEN 'DATETIME' -- datetime
				WHEN 62 THEN 'FLOAT64' -- float
				WHEN 98 THEN 'STRING' -- sql_variant
				WHEN 99 THEN 'STRING' -- ntext
				WHEN 104 THEN 'BOOL' -- bit
				WHEN 106 THEN 'FLOAT64' -- decimal
				WHEN 108 THEN 'FLOAT64' -- numeric
				WHEN 122 THEN 'FLOAT64' -- smallmoney
				WHEN 127 THEN 'INT64' -- bigint
				WHEN 240 THEN 'STRING' -- hierarchyid
				WHEN 240 THEN 'STRING' -- geometry
				WHEN 240 THEN 'STRING' -- geography
				WHEN 165 THEN 'BYTES' -- varbinary
				WHEN 167 THEN 'STRING' -- varchar
				WHEN 173 THEN 'BYTES' -- binary
				WHEN 175 THEN 'STRING' -- char
				WHEN 189 THEN 'STRING' -- timestamp
				WHEN 231 THEN 'STRING' -- nvarchar
				WHEN 239 THEN 'STRING' -- nchar
				WHEN 241 THEN 'STRING' -- xml
				WHEN 231 THEN 'STRING' -- sysname
		END bq_type FROM [{mssqlSincroHelper.database}].sys.tables t inner join [{mssqlSincroHelper.database}].sys.columns c on t.object_id = c.object_id
                    inner join [{mssqlSincroHelper.database}].sys.types ty on c.system_type_id = ty.system_type_id and c.user_type_id = ty.user_type_id
        WHERE t.name = '{mssqlSincroHelper.table}'

        OPEN c

        declare @name sysname, @bq_type varchar(100)

        fetch next from c into @name, @bq_type

        declare @schema varchar(max)='', @coma varchar(19) = ''
        while @@FETCH_STATUS=0
        BEGIN
            SET @schema += @coma + ' ' + @name  + ':' + @bq_type 
            SET @coma=','
            fetch next from c into @name, @bq_type
        END
        close c
        deallocate c
        SET @schema += ''
        SELECT @schema

    """
    schema = str(mssqlSincroHelper.get_value(sql))
    return schema

def get_bigquery_schema_changes_table(mssqlSincroHelper):
    schema = get_bigquery_schema(mssqlSincroHelper)
    schema = "cdc_operation:INT64," + schema + ", change_datetime:DATETIME, normalized_time:DATETIME"
    return schema

# sql =f"""
#     exec NeedFullrefresh @database_name=?,  @table_name=?,  @destination=?
# """

# database = 'HN_Ondata'
# table = 'CallTypes'
# destination = "sql"
# need_fullrefresh = get_mssql_value(sql, (database,table,destination))

# sql = """SELECT c.name FROM sys.tables t inner join sys.columns c on t.object_id = c.object_id
# 			inner join sys.types ty on c.system_type_id = ty.system_type_id and c.user_type_id = ty.user_type_id
# WHERE t.name = 'ODCalls'
# """
# cursor = get_mssql_cursor()
# cursor.execute(sql)
# result = cursor.fetchall()
# columns = []
# for c in result:
#     columns.append(c[0])



def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--mssql_instance',
                        help='Ip o host of the Sql Server Instance hosting the source.', required=True)
    parser.add_argument('--mssql_database',
                        help='Database containing the Table to replicate', required=True)
    parser.add_argument('--mssql_schema',
                        help='schema of the Table to replicate', default='dbo')
    parser.add_argument('--mssql_table',
                        help='Table to replicate', required=True)
    parser.add_argument('--mssql_port',
                        default=1433,
                        help='Sql Server Instance port')
    parser.add_argument('--mssql_login', required=True,
                        help='Sql Server Login')
    parser.add_argument('--mssql_password', required=True,
                        help='Sql Server Password')
    parser.add_argument('--bigquery_project',
                        help='GCP Project hosting the destination dataset ', required=True)
    parser.add_argument('--bigquery_dataset',
                        help='GCP dataset hosting the destination table ', required=True)
    parser.add_argument('--destination_table',
                        help='Destination table. If not supplied assume the same name as source table.')
    parser.add_argument('--config_table',
                        help='Table')
    known_args, pipeline_args = parser.parse_known_args()
    
    if known_args.destination_table is None:
        known_args.destination_table = known_args.mssql_table
    destination = f"{known_args.bigquery_project}.{known_args.bigquery_dataset}.{known_args.destination_table}"
    mssqlSincroHelper = MssqlSincroHelper(known_args.mssql_instance, known_args.mssql_port, known_args.mssql_login, known_args.mssql_password, known_args.mssql_database, known_args.mssql_table, known_args.mssql_schema)
    last_synced_lsn, last_unsynced_lsn = get_last_sync_info(known_args.config_table, mssqlSincroHelper, destination)
    full_refresh = need_full_refresh(known_args.config_table, mssqlSincroHelper, destination)
    if full_refresh:
        schema = get_bigquery_schema(mssqlSincroHelper)
        table = f'{known_args.bigquery_project}:{known_args.bigquery_dataset}.{known_args.destination_table}'
        write_disposition = BigQueryDisposition.WRITE_TRUNCATE
        last_unsynced_lsn = get_last_lsn(known_args.config_table, mssqlSincroHelper, destination)
    else:
        schema = get_bigquery_schema_changes_table(mssqlSincroHelper)
        table = f'{known_args.bigquery_project}:{known_args.bigquery_dataset}.{known_args.destination_table}_changes'
        write_disposition = BigQueryDisposition.WRITE_APPEND

    if last_synced_lsn is None and full_refresh == False:
        raise Exception(f"La sincronización no existe, esta deshabilitada o hay otra instancia corriendo revise la tabla {known_args.config_table}.")
    
    print(last_synced_lsn, last_unsynced_lsn)
    getMssqlChanges = GetMssqlChanges(mssqlSincroHelper, last_synced_lsn, last_unsynced_lsn, full_refresh)
    pipeline_options = PipelineOptions(pipeline_args)
    try:        
        with beam.Pipeline(options = pipeline_options) as p:
            result = (p
                    | 'Start' >> beam.Create([None])
                    | 'Get Changes' >> beam.ParDo(getMssqlChanges)
                    | 'Write to BigQuery' >> WriteToBigQuery(
                    table=table,
                    schema=schema,
                    create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=write_disposition)
            )

        if full_refresh == False:
            pk_condition =""
            separator = ""
            for s in mssqlSincroHelper.pk_columns:
                pk_condition += f" {separator} t.{s} = changes.{s} "
                separator = " AND "
            set_update = ""
            separator = ""
            for s in mssqlSincroHelper.cols_sin_pk():
                set_update += f" {separator} {s} = changes.{s} "
                separator = ", "
            
            sql=f"""
                MERGE `{known_args.bigquery_project}.{known_args.bigquery_dataset}.{known_args.destination_table}` t
                USING (SELECT * EXCEPT(rn) FROM (
                    SELECT *
                    ,row_number() over(Partition BY ID ORDER BY change_datetime DESC)   rn
                    FROM `{known_args.bigquery_project}.{known_args.bigquery_dataset}.{known_args.destination_table}_changes`
                    WHERE normalized_time IS NULL
                    ) tc WHERE rn = 1)  as changes
                ON {pk_condition}
                WHEN MATCHED AND changes.cdc_operation = 1 THEN
                DELETE
                WHEN MATCHED AND changes.cdc_operation != 1 THEN
                UPDATE SET {set_update}
                WHEN NOT MATCHED BY TARGET THEN
                INSERT ({mssqlSincroHelper.cols_coma_delimited()})
                VALUES ({mssqlSincroHelper.cols_coma_delimited()})
                ;

                UPDATE `{known_args.bigquery_project}.{known_args.bigquery_dataset}.{known_args.destination_table}_changes` SET normalized_time = current_datetime()  WHERE normalized_time IS NULL;
    --            FROM (
    --            SELECT *
    --            ,row_number() over(Partition BY ID ORDER BY change_datetime DESC)   rn
    --            FROM `{known_args.bigquery_project}.{known_args.bigquery_dataset}.{known_args.destination_table}_changes`
    --            ) tc WHERE rn = 1
    --                ;
                """
            result = bigquery_client.query(sql)
            print (result)

        sql = f"""
            UPDATE {known_args.config_table} SET last_sync_end = GETDATE(), last_lsn_synced = {last_unsynced_lsn}  
            WHERE database_name='{mssqlSincroHelper.database}' AND table_name = '{mssqlSincroHelper.table}' and destination = '{destination}' 
			AND Active = 1 AND last_sync_end IS NULL 
        """
        cursor = mssqlSincroHelper.get_cursor()
        result = cursor.execute(sql)
        print (result)
    except: 
        raise

    
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()


