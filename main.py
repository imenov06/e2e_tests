from brt_utils import connect_brt_db
from tests.test_e2e_classic_01 import test_e2e_classic_01

if __name__ == '__main__':
    conn = connect_brt_db()
    test_e2e_classic_01(conn)