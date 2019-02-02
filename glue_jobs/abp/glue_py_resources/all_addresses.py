import json
from pyspark.sql.utils import AnalysisException


def create_views_on_parquet(spark):

    with open("table_names.json", "r") as f:
        table_names = json.load(f)

    for k,v in table_names.items():
        try:
            df = spark.read.parquet("s3a://alpha-everyone/deleteathenaout/abpparquet/{}".format(v["tablename"]))
        except AnalysisException:
            continue

        df.createOrReplaceTempView(v["tablename"])


def geographic_addresses(spark):
    sql = """

    SELECT
    b.uprn as uprn,
    b.postcode_locator as postcode,
    CLASSIFICATION_CODE,
    b.latitude,
    b.longitude,


    concat(case when o.organisation is not null then concat(o.organisation, ' ') else '' end
    -- Secondary Addressable Information
    ,case when l.sao_text is not null then concat(l.sao_text, ' ') else '' end
    -- case statement for different combinations of the sao start numbers (e.g. if no sao start suffix)
    ,case
    when l.sao_start_number is not null and l.sao_start_suffix is null and l.sao_end_number is null
    then concat(l.sao_start_number, ' ')
    when l.sao_start_number is null then '' else concat(l.sao_start_number, '') end
    -- case statement for different combinations of the sao start suffixes (e.g. if no sao end number)
    ,case
    when l.sao_start_suffix is not null and l.sao_end_number is null then concat(l.sao_start_suffix, ' ')
    when l.sao_start_suffix is not null and l.sao_end_number is not null then l.sao_start_suffix else '' end
    -- Add a '-' between the start and end of the secondary address (e.g. only when sao start and sao end)
    ,case
    when l.sao_end_suffix is not null and l.sao_end_number is not null then '-'
    when l.sao_start_number is not null and l.sao_end_number is not null then '-'else '' end
    -- case statement for different combinations of the sao end numbers and sao end suffixes
    ,case
    when l.sao_end_number is not null and l.sao_end_suffix is null then concat(l.sao_end_number, ' ')
    when l.sao_end_number is null then '' else l.sao_end_number end
    -- pao end suffix
    ,case when l.sao_end_suffix is not null then concat(l.sao_end_suffix, ' ') else '' end
    -- Primary Addressable Information
    ,case when l.pao_text is not null then concat(l.pao_text, ' ') else '' end
    -- case statement for different combinations of the pao start numbers (e.g. if no pao start suffix)
    ,case
    when l.pao_start_number is not null and l.pao_start_suffix is null and l.pao_end_number is null
    then concat(l.pao_start_number, ' ')
    when l.pao_start_number is null then ''
    else concat(l.pao_start_number, '') end
    -- case statement for different combinations of the pao start suffixes (e.g. if no pao end number)
    ,case
    when l.pao_start_suffix is not null and l.pao_end_number is null then concat(l.pao_start_suffix, ' ')
    when l.pao_start_suffix is not null and l.pao_end_number is not null then l.pao_start_suffix
    else '' end
    -- Add a '-' between the start and end of the primary address (e.g. only when pao start and pao end)
    ,case
    when l.pao_end_suffix is not null and l.pao_end_number is not null then '-'
    when l.pao_start_number is not null and l.pao_end_number is not null then '-'
    else '' end
    -- case statement for different combinations of the pao end numbers and pao end suffixes
    ,case
    when l.pao_end_number is not null and l.pao_end_suffix is null then concat(l.pao_end_number, ' ')
    when l.pao_end_number is null then ''
    else l.pao_end_number end
    -- pao end suffix
    ,case when l.pao_end_suffix is not null then concat(l.pao_end_suffix, ' ') else '' end
    -- Street Information
    ,case when s.street_description is not null then concat(s.street_description, ' ') else '' end
    -- Locality-----
    ,case when s.locality_name is not null then concat(s.locality_name, ' ') else '' end

    -- Town--------
    ,case when s.town_name is not null then concat(s.town_name, ' ') else '' end
    -- Postcode---
    ,case when b.postcode_locator is not null then b.postcode_locator else '' end)
    AS full_address

    FROM
    abp_street_descriptor AS s,
    abp_classification as c,
    abp_lpi as l
    full outer join abp_organisation AS o on (l.uprn = o.uprn),
    abp_blpu AS b


    WHERE b.uprn = l.uprn and b.filename=l.filename
    AND l.usrn = s.usrn and l.filename=s.filename
    AND b.uprn = c.uprn and b.filename=c.filename
    and ADDRESSBASE_POSTAL != 'N'
    """

    geographic_addresses =  spark.sql(sql)
    return geographic_addresses


def delivery_addresses(spark):

    sql = """

    SELECT
    d.uprn as uprn,
    postcode,
    CLASSIFICATION_CODE,
    b.latitude,
    b.longitude,
    concat(
    CASE WHEN department_name IS NOT NULL THEN concat(department_name, ' ') ELSE '' END
    , CASE WHEN organisation_name IS NOT NULL THEN concat(organisation_name, ' ') ELSE '' END
    , CASE WHEN sub_building_name IS NOT NULL THEN concat(sub_building_name, ' ') ELSE '' END
    , CASE WHEN building_name IS NOT NULL THEN concat(building_name, ' ') ELSE '' END
    , CASE WHEN building_number IS NOT NULL THEN concat(building_number, ' ') ELSE '' END
    , CASE WHEN po_box_number IS NOT NULL THEN concat('PO BOX ', po_box_number, ' ') ELSE '' END
    , CASE WHEN dependent_thoroughfare IS NOT NULL THEN concat(dependent_thoroughfare, ' ') ELSE '' END
    , CASE WHEN thoroughfare IS NOT NULL THEN concat(thoroughfare, ' ') ELSE '' END
    , CASE WHEN double_dependent_locality IS NOT NULL THEN concat(double_dependent_locality, ' ') ELSE '' END
    , CASE WHEN dependent_locality IS NOT NULL THEN concat(dependent_locality , ' ') ELSE '' END
    , CASE WHEN post_town IS NOT NULL THEN concat(post_town, ' ') ELSE '' END
    , postcode
    ) AS full_address
    FROM abp_delivery_point as d
    left join abp_blpu as b
    on d.uprn=b.uprn and d.filename = b.filename
    left join abp_classification as c
    on b.uprn = c.uprn and b.filename = c.filename
    """

    delivery_addresses =  spark.sql(sql)
    return delivery_addresses


def create_all_addresses(spark):
    create_views_on_parquet(spark)
    geog = geographic_addresses(spark)
    deliv = delivery_addresses(spark)
    all_addresses = geog.union(deliv)
    all_addresses.write.parquet("s3a://alpha-everyone/deleteathenaout/abpparquet/abp_all_addresses", mode="overwrite")
