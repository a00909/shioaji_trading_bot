from data_manager.history.htm2._api_fetcher import ApiFetcher

fst = (1, 99)
sec = (50, 100)

is_ov, (stov, edov) = ApiFetcher._get_overlap_interval(
    fst[0], fst[1],
    sec[0], sec[1],
)



print(is_ov,stov, edov)
