SELECT
    CP.AccountID,
    A.AccountNum,
    A.DateOpen,
    CP.ClientId,
    C.ClientName,
    CAST((CP.PaymentAmt + EnrollementAmt) as DECIMAL(16, 2)) as TotalAmt,
    CP.CutoffDate
FROM corporate_payments CP
INNER JOIN Account A ON CP.AccountId = A.AccountId
INNER JOIN Client C ON C.Clientid = CP.ClientId

