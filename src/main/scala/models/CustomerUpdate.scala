package models

import java.sql.Date

case class CustomerUpdate(customerId: Long, address: String, effectiveDate: Date)
