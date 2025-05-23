from scripts.bank_glue_etl_job import customer_categorization


# --- UDF Test
def test_customer_segmentation_udf():
    assert customer_categorization(20, 5000) == "Youth Low"
    assert customer_categorization(50, 150000) == "Senior High"
    assert customer_categorization(35, 40000) == "General"
    assert customer_categorization(None, 20000) == "Unknown"
