class AdAmount:
    def __init__(
            self,
            ws_idx,
            openlisting_paid_pay_amount,
            openlisting_free_pay_amount,
            openlisting_total_payment,
            searchlisting_paid_pay_amount,
            searchlisting_free_pay_amount,
            searchlisting_total_payment,
            video_paid_pay_amount,
            video_free_pay_amount,
            video_total_payment,
            brand_paid_pay_amount,
            brand_free_pay_amount,
            brand_total_payment,
            paid_pay_amount,
            free_pay_amount,
            total_payment
    ):
        self.ws_idx = ws_idx
        self.openlisting_paid_pay_amount = openlisting_paid_pay_amount
        self.openlisting_free_pay_amount = openlisting_free_pay_amount
        self.openlisting_total_payment = openlisting_total_payment
        self.searchlisting_paid_pay_amount = searchlisting_paid_pay_amount
        self.searchlisting_free_pay_amount = searchlisting_free_pay_amount
        self.searchlisting_total_payment = searchlisting_total_payment
        self.video_paid_pay_amount = video_paid_pay_amount
        self.video_free_pay_amount = video_free_pay_amount
        self.video_total_payment = video_total_payment
        self.brand_paid_pay_amount = brand_paid_pay_amount
        self.brand_free_pay_amount = brand_free_pay_amount
        self.brand_total_payment = brand_total_payment
        self.paid_pay_amount = paid_pay_amount
        self.free_pay_amount = free_pay_amount
        self.total_payment = total_payment

    @classmethod
    def getInstance(self, row):
        return AdAmount(
            row[0][0], row[0][1], row[0][2], row[0][3],
            row[0][4], row[0][5], row[0][6], row[0][7],
            row[0][8], row[0][9], row[0][10], row[0][11],
            row[0][12], row[0][13], row[0][14], row[0][15]
        )
