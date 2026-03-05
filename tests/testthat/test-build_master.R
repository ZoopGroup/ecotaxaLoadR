testthat::test_that("build_master matches HEX tow with leading zeros", {
  pro_data <- tibble::tibble(
    vessel = "SR",
    cruise_id = "SR2408",
    tow_number = 1,
    net = 1,
    lat = 30,
    lon = -120,
    `CTDDEPTH(M)` = 100,
    vol = 10,
    datetime_gmt = as.POSIXct("2024-01-01 00:00:00", tz = "UTC"),
    datetime_local = as.POSIXct("2023-12-31 16:00:00", tz = "UTC"),
    is_day = TRUE
  )

  hex_data <- tibble::tibble(
    tow = "01",
    net_size = 1.0
  )

  result <- build_master(pro_data, hex_data)

  testthat::expect_equal(nrow(result), 1)
  testthat::expect_identical(result$tow, 1L)
  testthat::expect_identical(result$net, 1L)
  testthat::expect_equal(result$net_size, 1)
  testthat::expect_true("date_start_gmt" %in% names(result))
  testthat::expect_s3_class(result$date_start_gmt, "Date")
  testthat::expect_equal(result$date_start_gmt, as.Date("2024-01-01"))
})


testthat::test_that("build_master joins by tow and net when HEX net is available", {
  pro_data <- tibble::tibble(
    vessel = c("SR", "SR"),
    cruise_id = c("SR2408", "SR2408"),
    tow_number = c(1, 1),
    net = c(1, 2),
    lat = c(30, 30),
    lon = c(-120, -120),
    `CTDDEPTH(M)` = c(100, 120),
    vol = c(10, 12),
    datetime_gmt = as.POSIXct(c("2024-01-01 00:00:00", "2024-01-01 00:10:00"), tz = "UTC"),
    datetime_local = as.POSIXct(c("2023-12-31 16:00:00", "2023-12-31 16:10:00"), tz = "UTC"),
    is_day = c(TRUE, TRUE)
  )

  hex_data <- tibble::tibble(
    tow = c("01", "01"),
    net = c("1", "2"),
    net_size = c(0.5, 1.0)
  )

  result <- build_master(pro_data, hex_data)

  testthat::expect_equal(nrow(result), 2)
  result_sorted <- dplyr::arrange(result, .data$net)
  testthat::expect_identical(result_sorted$net, c(1L, 2L))
  testthat::expect_equal(result_sorted$net_size, c(0.5, 1.0))
  testthat::expect_true("date_start_gmt" %in% names(result_sorted))
  testthat::expect_s3_class(result_sorted$date_start_gmt, "Date")
  testthat::expect_true(all(result_sorted$date_start_gmt == as.Date("2024-01-01")))
})
