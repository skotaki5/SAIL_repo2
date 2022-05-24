SELECT
    [d].[UPSOrderNumber]
  , [d].[ORDERNUMBER]
  , [d0].[ExceptionReason]
  , CASE
        WHEN [d].[DateTimeShipped] IS NULL
            THEN CAST(1 AS bit)
            ELSE CAST(0 AS bit)
    END
  , [d].[DateTimeShipped]
  , [d].[OriginAddress1]
  , [d].[OriginAddress2]
  , [d].[OriginCity]
  , [d].[OriginProvince]
  , [d].[OriginPostalCode]
  , [d].[OriginCountry]
  , [d].[DestinationAddress1]
  , [d].[DestinationAddress2]
  , [d].[DestinationCity]
  , [d].[DestinationProvince]
  , [d].[DestinationPostalcode]
  , [d].[DestinationCountry]
  , [d].[ServiceLevel]
  , [d].[CurrentMilestone]
  , CASE
        WHEN [d].[ActualScheduledDeliveryDateTime] IS NULL
            THEN CAST(1 AS bit)
            ELSE CAST(0 AS bit)
    END
  , [d].[ActualScheduledDeliveryDateTime]
  , [d].[ShippedDateTimeZone]
  , [d].[ActualScheduledDeliveryDateTimeZone]
  , [d1].[WAREHOUSE_CODE]
  , [d].[Account_number]
  , [d].[AccountId]
FROM
    [Summary].[DIGITAL_SUMMARY_ORDERS] AS [d]
    INNER JOIN
        [Summary].[DIGITAL_SUMMARY_EXCEPTIONS] AS [d0]
        ON
            (
                (
                    [d].[UPSOrderNumber] = [d0].[UPSOrderNumber]
                )
                OR
                (
                    [d].[UPSOrderNumber]      IS NULL
                    AND [d0].[UPSOrderNumber] IS NULL
                )
            )
            AND
            (
                [d].[SourceSystemKey] = [d0].[SourceSystemKey]
            )
    INNER JOIN
        [dbo].[DIM_WAREHOUSE] AS [d1]
        ON
            (
                (
                    [d].[FacilityId] = [d1].[GLD_WAREHOUSE_MAPPED_KEY]
                )
                OR
                (
                    [d].[FacilityId]                    IS NULL
                    AND [d1].[GLD_WAREHOUSE_MAPPED_KEY] IS NULL
                )
            )
            AND
            (
                [d].[SourceSystemKey] = [d1].[SOURCE_SYSTEM_KEY]
            )
    INNER JOIN
        [Summary].[DIGITAL_SUMMARY_MILESTONE_ACTIVITY] AS [d2]
        ON
            (
                [d].[SourceSystemKey] = [d2].[SourceSystemKey]
            )
            AND
            (
                (
                    [d].[UPSOrderNumber] = [d2].[UPSOrderNumber]
                )
                OR
                (
                    [d].[UPSOrderNumber]      IS NULL
                    AND [d2].[UPSOrderNumber] IS NULL
                )
            )
WHERE
    (
        (
            (
                (
                    (
                        (
                            (
                                (
                                    (
                                        [d].[DateTimeReceived] IS NOT NULL
                                        AND
                                        (
                                            [d].[DateTimeReceived] >= @__start_0
                                        )
                                    )
                                    AND
                                    (
                                        [d].[DateTimeReceived] <= @__end_1
                                    )
                                )
                                AND
                                (
                                    [d].[AccountId] IS NOT NULL
                                    AND
                                    (
                                        UPPER([d].[AccountId]) = @__accountId
                                    )
                                )
                            )
                            AND [d0].[ExceptionReason] IS NOT NULL
                        )
                        AND
                        (
                            [d].[IS_INBOUND] = 0
                        )
                    )
                    AND
                    (
                        [d0].[ExceptionPrimaryIndicator] = 1.0
                    )
                )
                AND [d0].[ExceptionCategory] IS NOT NULL
            )
            AND
            (
                [d0].[ExceptionCategory] <> N'Save'
            )
        )
        AND
        (
            UPPER([d2].[ActivityCode]) = N'DELIVER'
        )
    )
    AND [d2].[ActivityDate] IS NOT NULL
ORDER BY
    [d].[DateTimeReceived] DESC