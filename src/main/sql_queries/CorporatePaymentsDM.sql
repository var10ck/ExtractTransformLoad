-- CTE for PaymentAmt, used to join operations with Rates, choosing nearest past date
WITH AllPmt_Rate AS (
    SELECT * FROM
    (
        SELECT
            AccountDB,
            AccountCR,
            O.Currency,
            DateOp,
            Amount,
            R.Rate,
            Comment,
    --         R.RateDate,
            RANK() OVER (PARTITION BY AccountDB, O.DateOp, O.Currency ORDER BY R.RateDate DESC) as rn
        FROM
            Operation O
            INNER JOIN Rate R ON O.Currency = R.Currency AND R.RateDate <= O.DateOp
        )
    WHERE rn = 1
),
-- CTE for PaymentAmt, calculating SUM of payments in different rates using CTE AllPmt_Rate
PmtAmt_CalcNatRate AS (
    SELECT
            AccountDB,
            AccountCR,
            DateOp,
    --         (SUM(PA_R.Amount) OVER (PARTITION BY PA_R.AccountDB, PA_R.Currency) * PA_R.Rate) as AmtInNatRate
            (SUM(Amount) * MAX(Rate)) as AmtInNatRate,
            Comment
    FROM AllPmt_Rate
    GROUP BY
        AccountDB,
        AccountCR,
        DateOp,
        Currency,
        Comment
),
-- CTE for PaymentAmt, calculating SUM of payments using CTE AllPmt_Rate
PmtAmt AS (
    SELECT
        AccountDB as AccountID,
        DateOp,
        CAST(SUM(AmtInNatRate) AS DECIMAL(16, 2)) as PmtAmt
    FROM PmtAmt_CalcNatRate
    GROUP BY
        AccountDB,
        DateOp
),
-- CTE for EnrollmentAmt, used to join operations with Rates, choosing nearest past date
AllEnr_Rate AS (
    SELECT * FROM
    (
        SELECT
            AccountDB,
            AccountCR,
            O.Currency,
            DateOp,
            Amount,
            R.Rate,
            Comment,
            RANK() OVER (PARTITION BY AccountCR, O.DateOp, O.Currency ORDER BY R.RateDate DESC) as rn
        FROM
            Operation O
            INNER JOIN Rate R ON O.Currency = R.Currency
        )
    WHERE rn = 1
),
-- CTE for EnrollmentAmt, calculating SUM of payments in different rates using CTE AllEnr_Rate
-- EnrAmt_CalcNatRate AS (
--     SELECT
--         AccountCR,
--         DateOp,
--         (SUM(Amount) * MAX(Rate)) as AmtInNatRate
--     FROM
--         AllEnr_Rate
--     GROUP BY
--         AccountCR,
--         DateOp,
--         Currency
-- ),
-- CTE for EnrollmentAmt, calculating SUM of payments using CTE AllEnr_Rate
EnrAmt AS (
    SELECT
        AccountCR as AccountID,
        DateOp,
        CAST(SUM(AmtInNatRate) AS DECIMAL(16,2)) as EnrAmt
    FROM
        PmtAmt_CalcNatRate
    GROUP BY
        AccountCR,
        DateOp
),
-- CTE for TaxAmt
TaxAmtCTE as (
    SELECT
        AccountDB as AccountID,
--         AccountNum,
        DateOp,
        CAST(SUM(AmtInNatRate) AS DECIMAL(16, 2)) as TaxAmt
    FROM (
        SELECT
            PA_C.AccountDB,
--             PA_C.AccountCR,
--             A.AccountNum,
            PA_C.DateOp,
            PA_C.AmtInNatRate
        FROM
            PmtAmt_CalcNatRate PA_C
            JOIN Account A ON PA_C.AccountCR = A.AccountId
        WHERE
            A.AccountNum LIKE '40702%'
    )
    GROUP BY
        AccountDB,
--         AccountNum,
        DateOp
),
ClearAmt AS (
    SELECT
        AccountCR as AccountID,
        DateOp,
        CAST(SUM(AmtInNatRate) AS DECIMAL(16, 2)) as ClearAmt
    FROM (
        SELECT
--             PA_C.AccountDB,
            PA_C.AccountCR,
--             A.AccountNum,
            PA_C.DateOp,
            PA_C.AmtInNatRate
        FROM
            PmtAmt_CalcNatRate PA_C
            JOIN Account A ON PA_C.AccountDB = A.AccountId
        WHERE
            A.AccountNum LIKE '40802%'
    )
    GROUP BY
        AccountCR,
        DateOp
)
,
CarsAmt AS (
    SELECT
        AccountDB AS AccountID,
        DateOp,
        CAST(SUM(AmtInNatRate) AS DECIMAL(16, 2)) as CarsAmt
    FROM (
        SELECT
            AccountDB,
            DateOp,
            AmtInNatRate
        FROM
            PmtAmt_CalcNatRate PAC_1
        WHERE NOT EXISTS (SELECT id FROM calculation_params_tech CPT
            WHERE CPT.list_num = 1 AND PAC_1.Comment LIKE CPT.mask_string)
            )
    GROUP BY
        AccountDB,
        DateOp
)
,
FoodAmt AS (
    SELECT
        AccountCR AS AccountID,
        DateOp,
        CAST(SUM(AmtInNatRate) AS DECIMAL(16, 2)) as FoodAmt
    FROM (
        SELECT
            AccountCR,
            DateOp,
            AmtInNatRate
        FROM
            PmtAmt_CalcNatRate PAC_1
        WHERE EXISTS (SELECT id FROM calculation_params_tech CPT
            WHERE CPT.list_num = 2 AND PAC_1.Comment LIKE CPT.mask_string)
            )
    GROUP BY
        AccountCR,
        DateOp
)
,
FLAmt AS (
    SELECT
        AccountDB as AccountID,
--         ClientId,
        DateOp,
        CAST(SUM(AmtInNatRate) AS DECIMAL(16, 2)) as FLAmt
    FROM (
        SELECT
            PA_C.AccountDB,
--             PA_C.AccountCR,
--             Cl.ClientID,
            A.AccountNum,
            PA_C.DateOp,
            PA_C.AmtInNatRate
        FROM
            PmtAmt_CalcNatRate PA_C
            JOIN Account A ON PA_C.AccountCR = A.AccountId
            JOIN Client Cl ON A.ClientId = Cl.ClientId
        WHERE
            Cl.Type = 'Ф'
    )
    GROUP BY
        AccountDB,
--         ClientID,
        DateOp
)

SELECT
    PA.AccountID,
    A.ClientId,
    PA.PmtAmt as PaymentAmt,
    EA.EnrAmt as EnrollementAmt,
    TA.TaxAmt as TaxAmt,
    ClA.ClearAmt as ClearAmt,
    CA.CarsAmt as CarsAmt,
    FA.FoodAmt as FoodAmt,
    FLA.FlAmt as FLAmt,
    PA.DateOp as CutoffDate

FROM
    PmtAmt PA
    LEFT JOIN EnrAmt EA ON PA.AccountID = EA.AccountID
    LEFT JOIN TaxAmtCTE TA ON TA.AccountID = PA.AccountID
    LEFT JOIN ClearAmt ClA ON ClA.AccountID = PA.AccountID
    LEFT JOIN CarsAmt CA ON CA.AccountID = PA.AccountID
    LEFT JOIN FoodAmt FA ON FA.AccountID = PA.AccountID
    LEFT JOIN FLAmt FLA ON FLA.AccountID = PA.AccountID
    LEFT JOIN Account A ON PA.AccountID = A.AccountID
ORDER BY AccountID
-- SELECT * FROM FLAmt
-- ORDER BY AccountID

