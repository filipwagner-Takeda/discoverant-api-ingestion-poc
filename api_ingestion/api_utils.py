import requests


def find_valid_pages(base_url, start_page, end_page, page_param="page", headers=None, auth=None):
    """
    Recursively checks page ranges and returns pages that contain data.
    Stops when a page returns 404 or empty data.
    """
    valid_pages = []

    def check_range(s, e):
        if s > e:
            return

        # Check the first page in the range
        params = {page_param: s}
        resp = requests.get(base_url, params=params, headers=headers, auth=auth, timeout=10)

        if resp.status_code == 404:
            return

        data = resp.json()
        if not data or len(data) == 0:
            return

        if s == e:
            valid_pages.append(s)
            return

        mid = (s + e) // 2
        check_range(s, mid)
        check_range(mid + 1, e)

    check_range(start_page, end_page)
    return max(valid_pages)