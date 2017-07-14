val dateDimTable = "dimensional_warehouse.dim_user"
sql(s"DROP TABLE IF EXISTS ${dateDimTable} PURGE")


import org.alghimo.spark.dimensionalModeling.dimensions.{UserDimension, UserDimensionLoader, EnrichedUserDimension, NaturalUserDimension}
import java.sql.Timestamp
import org.joda.time.DateTime
/**
 * Test with date dimension
 */
val loader = new UserDimensionLoader(dateDimTable, spark)
val fullTableName = loader.dimensionTableName
val Array(dimDbName, dimTableName) = fullTableName.split('.')

if (!spark.catalog.databaseExists(dimDbName)) {
    sql("CREATE SCHEMA dimensional_warehouse LOCATION '/apps/hive/warehouse/dimensional_warehouse.db'")
}

if (!spark.catalog.tableExists(dimDbName, dimTableName)) {
    // Create empty table
    spark
        .createDataset(Seq[UserDimension]())
        .write
        .saveAsTable(fullTableName)
}

val dt = new DateTime("2017-05-26")
val baseTs = System.currentTimeMillis()
val oneDayMillis: Long = 1000L * 60 * 60 *24

val users = Seq(
  EnrichedUserDimension(
    dim_timestamp        = new Timestamp(baseTs - 33 * oneDayMillis),
    id                   = 1,
    email                = "foo@example.com",
    sign_in_count        = 7,
    created_at           = new Timestamp(dt.minusDays(59).getMillis),
    receive_mail         = true,
    has_seen_campus_tour = false,
    coursera             = false,
    has_anonymous_email  = false,
    company_name         = None,
    updated_at           = None,
    education            = Some("BS"),
    first_name           = Some("foo"),
    from_invite          = false,
    last_name            = Some("bar"),
    location             = Some("Belgium"),
    lti_src              = None,
    name                 = Some("foo bar"),
    has_braintree        = false,
    has_paypal           = true,
    has_remember         = false,
    has_stripe           = false,
    reset_password_sent  = false,
    src                  = None
  ),
  EnrichedUserDimension(
    dim_timestamp        = new Timestamp(baseTs - 13 * oneDayMillis),
    id                   = 2,
    email                = "foo2@example.com",
    sign_in_count        = 5,
    created_at           = new Timestamp(dt.minusDays(13).getMillis),
    receive_mail         = true,
    has_seen_campus_tour = false,
    coursera             = false,
    has_anonymous_email  = false,
    company_name         = None,
    updated_at           = None,
    education            = Some("MS"),
    first_name          = Some("foo2"),
    from_invite          = false,
    last_name            = Some("bar"),
    location             = Some("Catalonia"),
    lti_src              = None,
    name                 = Some("foo2 bar"),
    has_braintree        = false,
    has_paypal           = true,
    has_remember         = false,
    has_stripe           = false,
    reset_password_sent  = false,
    src                  = None
  )
)

val ds = spark.createDataset(users)

val dimensionTable = loader.dimensionTable()

// Extract new dims
val newDims = loader.newDimensions(ds)
assert(newDims.count == 2, "First step - new dims should be 2")

// Get new dim table
val extractedDims = loader.extractDimensions(ds)
assert(extractedDims.count == 2, "Extracted dims should have 2 rows")
val newDimensionTable = loader.extractDimensionsAndSave(ds)
assert(newDimensionTable.count == 2, "New dimension table should have 2 rows")
val dimensionTable = loader.dimensionTable()

val newDims = loader.newDimensions(ds)
assert(newDims.count == 0, "Reprocessing same input shouldn't give new dimensions") // 0

val ds2 = spark.createDataset(Seq(
  EnrichedUserDimension(
    NaturalUserDimension(
      id                   = 3,
      email                = "foo3@example.com",
      sign_in_count        = 5,
      created_at           = new Timestamp(dt.minusDays(13).getMillis),
      biography = "",
      receive_mail = true,
      has_seen_campus_tour = false,
      coursera = false,
      has_anonymous_email = false,
      updated_at = None,
      active_payment_method_token = None,
      braintree_customer_id = Some("123"),
      company_name = Some("mu company"),
      current_active_group_hash = None,
      current_sign_in_at = None,
      last_sign_in_at = None,
      current_sign_in_ip = None,
      last_sign_in_ip = None,
      deleted_at = None,
      education            = Some("MS"),
      first_name = Some("foo"),
      invite_created_at = Some(new Timestamp(dt.minusDays(15).getMillis)),
      invite_channel = None,
      invite_system = None,
      inviter_id = None,
      last_name = Some("bar"),
      location             = Some("Catalonia"),
      lti_src = None,
      marketing_biography = None,
      name = Some("boo bar"),
      paypal_customer_id = None,
      remember_created_at = None,
      reset_password_sent_at = None,
      slug = None,
      src = None,
      stripe_customer_id = None,
      encrypted_password = "",
      authentication_token = None,
      avatar_file_name = None,
      avatar_content_type = None,
      avatar_file_size = None,
      avatar_updated_at = None,
      reset_password_token = None
    ),
    Some(new Timestamp(baseTs - 13 * oneDayMillis)))
))

val newDims = loader.newDimensions(ds2)
assert(newDims.count == 1, "ds2 has one new dimension")
val newDimensionTable = loader.extractDimensionsAndSave(ds2)
val dimensionTable = loader.dimensionTable()
assert(dimensionTable.count==3, "dimension table should have 3 rows after ds2")

val ds3 = spark.createDataset(Seq(
  // Only the timestamp is different, so this should be considered "unchanged"
  EnrichedUserDimension(
    dim_timestamp        = new Timestamp(baseTs - 12 * oneDayMillis),
    id                   = 2,
    email                = "foo2@example.com",
    sign_in_count        = 5,
    created_at           = new Timestamp(dt.minusDays(13).getMillis),
    receive_mail         = true,
    has_seen_campus_tour = false,
    coursera             = false,
    has_anonymous_email  = false,
    company_name         = None,
    updated_at           = None,
    education            = Some("MS"),
    first_name          = Some("foo2"),
    from_invite          = false,
    last_name            = Some("bar"),
    location             = Some("Catalonia"),
    lti_src              = None,
    name                 = Some("foo2 bar"),
    has_braintree        = false,
    has_paypal           = true,
    has_remember         = false,
    has_stripe           = false,
    reset_password_sent  = false,
    src                  = None
  ),
  // Type1 change
  EnrichedUserDimension(
    dim_timestamp        = new Timestamp(baseTs - 33 * oneDayMillis),
    id                   = 1,
    email                = "newfooemail@example.com",
    sign_in_count        = 7,
    created_at           = new Timestamp(dt.minusDays(59).getMillis),
    receive_mail         = true,
    has_seen_campus_tour = false,
    coursera             = false,
    has_anonymous_email  = false,
    company_name         = None,
    updated_at           = None,
    education            = Some("BS"),
    first_name           = Some("foo"),
    from_invite          = false,
    last_name            = Some("bar"),
    location             = Some("Belgium"),
    lti_src              = None,
    name                 = Some("foo bar"),
    has_braintree        = false,
    has_paypal           = true,
    has_remember         = false,
    has_stripe           = false,
    reset_password_sent  = false,
    src                  = None
  )
))

val newDims = loader.newDimensions(ds3)
assert(newDims.count == 0, "ds3 does not have new Dims!")
val (newType2Dims, updatedDims) = loader.updatedDimensions(ds3)
assert(newType2Dims.count == 0, "There should be 0 new type2 dimensions")// 3
assert(updatedDims.count == 1, "There should be 1 updated dimensions")// 3
val newDimensionTable = loader.extractDimensions(ds3)
assert(newDimensionTable.count == 3, "ds3 has type1 change - Dimension table should still have 3 rows")

val newDimensionTable = loader.extractDimensionsAndSave(ds3)
val dimensionTable = loader.dimensionTable()
assert(dimensionTable.count == 3, "ds3 has type1 change - Dimension table should still have 3 rows")

val ds4 = spark.createDataset(Seq(
  EnrichedUserDimension(
    dim_timestamp        = new Timestamp(baseTs - 10 * oneDayMillis),
    id                   = 3,
    email                = "foo3@example.com",
    sign_in_count        = 5,
    created_at           = new Timestamp(dt.minusDays(13).getMillis),
    receive_mail         = true,
    has_seen_campus_tour = true,
    coursera             = false,
    has_anonymous_email  = false,
    company_name         = None,
    updated_at           = None,
    education            = Some("MS"),
    first_name           = Some("foo3"),
    from_invite          = false,
    last_name            = Some("bar"),
    location             = Some("Catalonia"),
    lti_src              = None,
    name                 = Some("foo2 bar"),
    has_braintree        = false,
    has_paypal           = true,
    has_remember         = false,
    has_stripe           = false,
    reset_password_sent  = false,
    src                  = None
  )
))

val newDims = loader.newDimensions(ds4)
assert(newDims.count == 0, "ds4 does not have new Dims!")
val (newType2Dims, updatedDims) = loader.updatedDimensions(ds4)
assert(newType2Dims.count == 1, "There should be 1 new type2 dimensions")// 3
assert(updatedDims.count == 1, "There should be 1 updated dimensions")// 3
/*//Failed...
val enrichedDimensions = loader.keepOnlyMostRecentEvents(allEnrichedDimensions)
val currentDims = loader.currentDimensions()
val enrichedWithCurrentDims = loader.joinOnNaturalKeys(enrichedDimensions, currentDims, "inner")
val allUpdatedDims = enrichedWithCurrentDims.filter(e => loader.areDifferent(e._1, e._2))

val type1Updates = allUpdatedDims
      .filter(d => !loader.hasType2Changes(d))
      .map(loader.type1Change)
allUpdatedDims.filter(d => loader.hasType2Changes(d))*/


val newDimensionTable = loader.extractDimensions(ds4)
assert(newDimensionTable.count == 4, "ds4 has type2 change - Dimension table should have 4 rows now")
val newDimensionTable = loader.extractDimensionsAndSave(ds4)
val dimensionTable = loader.dimensionTable()
assert(dimensionTable.count == 4, "ds4 has type2 change - Dimension table should have 4 rows now")


// Test mixing a type 1 and type 2 change. Old row should remain unchanged, type1 applies only in new row
val ds5 = spark.createDataset(Seq(
  EnrichedUserDimension(
    dim_timestamp        = new Timestamp(baseTs - 10 * oneDayMillis),
    id                   = 3,
    email                = "foo3@example.com",
    sign_in_count        = 5,
    created_at           = new Timestamp(dt.minusDays(13).getMillis),
    receive_mail         = false, // This is the type2 change
    has_seen_campus_tour = true,
    coursera             = false,
    has_anonymous_email  = false,
    company_name         = None,
    updated_at           = None,
    education            = Some("MS"),
    first_name           = Some("foo3 changed"), // This is the type1 change
    from_invite          = false,
    last_name            = Some("bar"),
    location             = Some("Catalonia"),
    lti_src              = None,
    name                 = Some("foo2 bar"),
    has_braintree        = false,
    has_paypal           = true,
    has_remember         = false,
    has_stripe           = false,
    reset_password_sent  = false,
    src                  = None
  )
))

val newDims = loader.newDimensions(ds5)
assert(newDims.count == 0, "ds5 does not have new Dims!")
val (newType2Dims, updatedDims) = loader.updatedDimensions(ds5)
assert(newType2Dims.count == 1, "There should be 1 new type2 dimensions")// 3
assert(updatedDims.count == 1, "There should be 1 updated dimensions")// 3

val newDimensionTable = loader.extractDimensions(ds5)
assert(newDimensionTable.count == 5, "ds5 has type2 change - Dimension table should have 5 rows now")
val newDimensionTable = loader.extractDimensionsAndSave(ds5)
val dimensionTable = loader.dimensionTable()
assert(dimensionTable.count == 5, "ds5 has type2 change - Dimension table should have 5 rows now")

// Next tests:
// - 2 events on the same natural keys (only most recent should be kept)
// - 2 events on the same natural keys and timestamp (only one of them should be kept)
