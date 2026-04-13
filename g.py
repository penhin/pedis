import os
import sys

def run(cmd):
    print(f"\n> {cmd}")
    os.system(cmd)

def commit(msg):
    run("git add .")
    run(f'git commit -m "{msg}"')

def amend(msg=None):
    run("git add .")
    if msg:
        run(f'git commit --amend -m "{msg}"')
    else:
        run("git commit --amend --no-edit")

def push(force=True):
    if force:
        run("git push --force")
    else:
        run("git push")

def sync(msg=None):
    run("git add .")
    if msg:
        run(f'git commit --amend -m "{msg}"')
    else:
        run("git commit --amend --no-edit")
    run("git push --force")

def reset_one_commit(msg):
    run("git add .")
    run("git reset --soft $(git rev-list --max-parents=0 HEAD)")
    run(f'git commit -m "{msg}"')

def main():
    if len(sys.argv) < 2:
        print("""
Usage:
  python g.py commit "msg"
  python g.py amend ["msg"]
  python g.py sync ["msg"]
  python g.py push
  python g.py reset "msg"
        """)
        return

    cmd = sys.argv[1]

    if cmd == "commit":
        commit(sys.argv[2] if len(sys.argv) > 2 else "wip")

    elif cmd == "amend":
        amend(sys.argv[2] if len(sys.argv) > 2 else None)

    elif cmd == "sync":
        sync(sys.argv[2] if len(sys.argv) > 2 else None)

    elif cmd == "push":
        push()

    elif cmd == "reset":
        reset_one_commit(sys.argv[2] if len(sys.argv) > 2 else "final commit")

    else:
        print("Unknown command")

if __name__ == "__main__":
    main()