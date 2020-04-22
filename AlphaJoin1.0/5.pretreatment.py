import time

from arguments import get_args
from supervised import supervised

if __name__ == '__main__':
    args = get_args()

    trainer = supervised(args)
    print("Pretreatment running...")
    start = time.clock()
    trainer.pretreatment("./data/XXX.sql")
    elapsed = (time.clock() - start)
    print("Pretreatment time used:", elapsed)
