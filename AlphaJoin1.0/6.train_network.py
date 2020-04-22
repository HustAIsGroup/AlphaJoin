from arguments import get_args
from supervised import supervised

if __name__ == '__main__':
    args = get_args()
    trainer = supervised(args)
    trainer.supervised()
    trainer.test_network()