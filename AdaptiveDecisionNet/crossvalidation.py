from arguments import get_args
from supervised import supervised

if __name__ == '__main__':
    args = get_args()
    trainer = supervised(args)
    trainer.pretreatment("./data/runtime.csv")

    for i in range(trainer.datasetnumber):
        trainer.load_data(i)
        trainer.supervised()
        trainer.test_network()
        trainer.trainList.clear()
        trainer.testList.clear()