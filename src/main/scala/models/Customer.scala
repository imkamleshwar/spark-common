package models

import java.sql.Date

case class Customer(customerId: Long, address: String, current: Boolean, effectiveDate: Date, endDate: Date)
