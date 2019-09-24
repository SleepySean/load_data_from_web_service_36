import os, sys


def main():
    print("Hello1")


if __name__ == '__main__':
    main()

print(os.path.realpath(__file__))

print('sys.argv[0] =', sys.argv[0])
pathname = os.path.dirname(sys.argv[0])
print('path =', pathname)
print('full path =', os.path.abspath(pathname))
