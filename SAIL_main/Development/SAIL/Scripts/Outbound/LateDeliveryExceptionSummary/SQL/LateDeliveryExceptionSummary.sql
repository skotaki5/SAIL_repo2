SELECT
    [f].[EXCEPTION_REASON]      AS [LateDeliveryExceptionReason]
  , COUNT_BIG(*)                AS [Count]
  , [f].[EXCEPTION_REASON_TYPE] AS [ReasonType]
FROM
    [Summary].[DIGITAL_SUMMARY_ORDERS] AS [d]
    INNER JOIN
        [dbo].[FACT_TRANSPORTATION_EXCEPTION] AS [f]
        ON
            (
                (
                    [d].[UPSOrderNumber] = [f].[UPS_ORDER_NUMBER]
                )
                OR
                (
                    [d].[UPSOrderNumber]       IS NULL
                    AND [f].[UPS_ORDER_NUMBER] IS NULL
                )
            )
            AND
            (
                [d].[SourceSystemKey] = [f].[SOURCE_SYSTEM_KEY]
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
                                    [d].[DateTimeReceived] IS NOT NULL
                                    AND
                                    (
                                        [d].[DateTimeReceived] >= @__startDateTime_0
                                    )
                                )
                                AND
                                (
                                    [d].[DateTimeReceived] <= @__endDateTime_1
                                )
                            )
                            AND
                            (
                                [d].[AccountId] IS NOT NULL
                                AND
                                (
                                    UPPER([d].[AccountId]) = @__ToUpper_2
                                )
                            )
                        )
                        AND
                        (
                            [d].[FacilityId] IS NOT NULL
                            AND
                            (
                                UPPER([d].[FacilityId]) = @__ToUpper_3
                            )
                        )
                    )
                    AND
                    (
                        [d].[ActualDeliveryDate] > [d].[ActualScheduledDeliveryDateTime]
                    )
                )
                AND
                (
                    [d].[OrderCancelledFlag] = N'N'
                )
            )
            AND
            (
                [d].[IS_INBOUND] = 0
            )
        )
        AND
        (
            [f].[EXCEPTION_PRIMARY_INDICATOR] = 1.0
        )
    )
    AND
    (
        (
            [f].[EXCEPTION_CATEGORY] <> N'Save'
        )
        AND [f].[EXCEPTION_CATEGORY] IS NOT NULL
    )
GROUP BY
    [f].[EXCEPTION_REASON]
  , [f].[EXCEPTION_REASON_TYPE]