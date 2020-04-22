import argparse

# get the arguments...
def get_args():
    parse = argparse.ArgumentParser(description='nn')
    parse.add_argument('--env-name', type=str, default='postgresql', help='the training environment')
    parse.add_argument('--save-dir', type=str, default='saved_models/', help='the folder that save the models')

    # get args...
    args = parse.parse_args()

    return args
