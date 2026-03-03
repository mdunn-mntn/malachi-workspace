package com.mntn.idg.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.{functions => F}
import scala.reflect.runtime.universe._

sealed trait IDType {
  def code: Int
  def name: String
  def badValues: Option[Seq[String]] = None
  def validationPattern: Option[String] = None
  /**
    * Cleanses a column value according to this IDType's rules.
    * Performs the following steps in order:
    * 1. Trims whitespace and converts to lowercase
    * 2. Nulls out values in the badValues list
    * 3. Nulls out values that don't match the validationPattern
    * 
    * @param col The Spark Column to cleanse
    * @param additionalBadValues Optional additional bad values to filter out
    * @return A Spark Column with cleansing transformations applied
    */
  def cleanse(col: Column, additionalBadValues: Seq[String] = Seq.empty): Column = {
    var result = F.trim(F.lower(col))
    
    // Filter out bad values (from IDType + additional)
    val allBadValues = badValues.getOrElse(Seq.empty) ++ additionalBadValues
    if (allBadValues.nonEmpty) {
      result = F.when(!result.isin(allBadValues: _*), result)
    }
    
    // Validate against pattern
    validationPattern.foreach { pattern =>
      result = F.when(result.rlike(pattern), result)
    }
    
    result
  }
}

object IDType {
  object KnownBadValues {
    val ifa = Seq("00000000-0000-0000-0000-000000000000", "[device_id]", "unknown")
    val ipv4 = Seq("0.0.0.0", "127.0.0.1")
    val ipv6 = Seq("0:0:0:0:0:0:0:0", "::1")
  }

  object ValidationPatterns {
    /** UUID v4 format with hyphens */
    val uuid = "^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$"
    
    /** IPv4 address format */
    val ipv4 = "^((25[0-5]|(2[0-4]|1[0-9]|[1-9]|)[0-9])(\\.(?!$)|$)){4}$"
    
    /** IPv6 address format (supports full, compressed, and IPv4-mapped formats) */
    val ipv6 = "^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]+|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\\.){3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))$"
    
    /** SHA-256 hash (64 hex characters) */
    val sha256 = "^[a-f0-9]{64}$"
    
    /** SHA-1 hash (40 hex characters) */
    val sha1 = "^[a-f0-9]{40}$"
    
    /** MD5 hash (32 hex characters) */
    val md5 = "^[a-f0-9]{32}$"
    
    /** LUID - numeric identifier */
    val luid = "^[0-9]+$"

    /** GA client identifier (GA/GA4 cookie style, AMP ids, or hashed cookie fallbacks). */
    val gaClientId =
      "^(?:(ga1\\.\\d\\.)?\\d{4,}\\.\\d{10}|gs2\\.\\d\\..+|([0-9a-f]{4,12}\\b-?)+|amp-.+|(\\d{5,}\\.)+\\d{5,})$"
  }
  
  // Define all ID types as case objects
  // IDTypes starting in 1 are for hardware identifiers
  case object HARDWARE_IDFA extends IDType {
    val code = 10
    val name = "HARDWARE_IDFA"
    override val validationPattern = Some(ValidationPatterns.uuid)
    override val badValues = Some(KnownBadValues.ifa)
  }
  case object HARDWARE_ANDROID_AD_ID extends IDType {
    val code = 11
    val name = "HARDWARE_ANDROID_AD_ID"
    override val validationPattern = Some(ValidationPatterns.uuid)
    override val badValues = Some(KnownBadValues.ifa)
  }
  case object HARDWARE_TV extends IDType {
    val code = 12
    val name = "HARDWARE_TV"
    override val validationPattern = Some(ValidationPatterns.uuid)
    override val badValues = Some(KnownBadValues.ifa)
  }
  case object MOBILE_AD_ID extends IDType {
    val code = 13
    val name = "MOBILE_AD_ID"
    override val validationPattern = Some(ValidationPatterns.uuid)
    override val badValues = Some(KnownBadValues.ifa)
  }

  // IDTypes starting in 2 are for email-based identifiers
  case object HEM_SHA256 extends IDType {
    val code = 20
    val name = "HEM_SHA256"
    // SHA256 produces 64 hexadecimal characters
    override val validationPattern = Some(ValidationPatterns.sha256)
  }
  case object HEM_SHA1 extends IDType {
    val code = 21
    val name = "HEM_SHA1"
    // SHA1 produces 40 hexadecimal characters
    override val validationPattern = Some(ValidationPatterns.sha1)
  }
  case object HEM_MD5 extends IDType {
    val code = 22
    val name = "HEM_MD5"
    // MD5 produces 32 hexadecimal characters
    override val validationPattern = Some(ValidationPatterns.md5)
  }
  case object UID2 extends IDType {
    val code = 23
    val name = "UID2"
  }

  // IDTypes starting in 3 are for IP identifiers
  case object IPV4 extends IDType {
    val code = 30
    val name = "IPV4"
    override val validationPattern = Some(ValidationPatterns.ipv4)
    override val badValues = Some(KnownBadValues.ipv4)
  }
  case object IPV6 extends IDType {
    val code = 31
    val name = "IPV6"
    override val validationPattern = Some(ValidationPatterns.ipv6)
    override val badValues = Some(KnownBadValues.ipv6)
  }
  // Synthetic IP identifier used during graph generation to represent (IP, day).
  // This must remain stable to avoid IDType collisions across the pipeline.
  case object IP_DAY extends IDType {
    val code = 32
    val name = "IP_DAY"
  }
  
  // IDTypes starting in 4 are for cookie identifiers
  case object COOKIE extends IDType {
    val code = 40
    val name = "COOKIE"
  }
  case object MNTN_GUID extends IDType {
    val code = 41
    val name = "MNTN_GUID"
  }
  case object GA_CLIENT_ID extends IDType {
    val code = 42
    val name = "GA_CLIENT_ID"
    override val validationPattern = Some(ValidationPatterns.gaClientId)
  }

  // IDTypes starting in 5 are for Household identifiers
  case object LUID extends IDType {
    val code = 50
    val name = "LUID"
    override val validationPattern = Some(ValidationPatterns.luid)
  }
  
  // List of all types for iteration - automatically discovered via reflection
  val all: Seq[IDType] = {
    val mirror = runtimeMirror(getClass.getClassLoader)
    val idTypeSymbol = typeOf[IDType].typeSymbol.asClass
    
    idTypeSymbol.knownDirectSubclasses.toSeq.map { subclass =>
      val moduleMirror = mirror.reflectModule(subclass.asClass.module.asModule)
      moduleMirror.instance.asInstanceOf[IDType]
    }.sortBy(_.code)
  }
  
  // Conversion maps for efficient lookup
  private val codeToType: Map[Int, IDType] = all.map(t => t.code -> t).toMap
  private val nameToType: Map[String, IDType] = all.map(t => t.name -> t).toMap
  
  // Convert from integer code to IDType
  def fromCode(code: Int): Option[IDType] = codeToType.get(code)
  
  def fromCodeOrThrow(code: Int): IDType = 
    fromCode(code).getOrElse(throw new IllegalArgumentException(s"Unknown IDType code: $code"))
  
  // Convert from string name to IDType
  def fromName(name: String): Option[IDType] = nameToType.get(name.toUpperCase)
  
  def fromNameOrThrow(name: String): IDType = 
    fromName(name).getOrElse(throw new IllegalArgumentException(s"Unknown IDType name: $name"))
  
  // Spark UDFs for column operations
  val nameToCodeUDF = F.udf((name: String) => fromNameOrThrow(name).code)
  val codeToNameUDF = F.udf((code: Int) => fromCodeOrThrow(code).name)
  
  // Helper methods for Spark columns
  def toCode(nameColumn: Column): Column = nameToCodeUDF(nameColumn)
  def toName(codeColumn: Column): Column = codeToNameUDF(codeColumn)
  
  // Implicit ordering for sorting
  implicit val ordering: Ordering[IDType] = Ordering.by(_.code)
}
