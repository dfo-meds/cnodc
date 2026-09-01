from pipeman.programs.woa.climatology_check import WorldOceanAtlasClimatologyCheck, WOATimeResolution


class GTSPPWOASeasonalCheck(WorldOceanAtlasClimatologyCheck):

    def __init__(self):
        super().__init__(
            test_protocol="gtspp",
            test_name="woa_seasonal_2023",
            test_version="1.0",
            temporal_resolution=WOATimeResolution.SEASONAL,
            std_dev_range=3,
            test_tags=["GTSPP_3.1"],
        )


class GTSPPWOAMonthlyCheck(WorldOceanAtlasClimatologyCheck):

    def __init__(self):
        super().__init__(
            test_protocol="gtspp",
            test_name="woa_monthly_2023",
            test_version="1.0",
            temporal_resolution=WOATimeResolution.MONTHLY,
            std_dev_range=3,
            test_tags=["GTSPP_3.4"],
        )