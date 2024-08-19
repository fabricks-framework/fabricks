from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, StringType, StructField, StructType

from fabricks.core.udfs import udf


@udf(name="parse_phone_number")
def parse_phone_number(spark: SparkSession):
    def _parse_phone_number(phone_number: str, country: str = "CH"):
        from phonenumbers import PhoneNumberFormat  # type: ignore
        from phonenumbers import PhoneNumberMatcher  # type: ignore
        from phonenumbers import format_number  # type: ignore

        number = None
        for match in PhoneNumberMatcher(phone_number, country):
            number = match.number

        return {
            "valid": True,
            "clean_phone_nr": format_number(number, PhoneNumberFormat.E164),
            "original_phone_nr": phone_number,
            "country": country,
        }

    spark.udf.register(
        "udf_parse_phone_number",
        _parse_phone_number,
        StructType(
            [
                StructField("valid", BooleanType()),
                StructField("clean_phone_nr", StringType()),
                StructField("original_phone_nr", StringType()),
                StructField("country", StringType()),
            ]
        ),
    )
