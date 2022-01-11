SELECT
    C.ClientId,
    C.ClientName,
    C.Type,
    C.Form,
    C.RegisterDate,
    SUM(CA.TotalAmt) as TotalAmt,
    CA.CutoffDate
FROM Client C
INNER JOIN corporate_account CA ON CA.ClientID = C.ClientId
GROUP BY
    C.ClientId,
    C.ClientName,
    C.Type,
    C.Form,
    C.RegisterDate,
    CA.CutoffDate

