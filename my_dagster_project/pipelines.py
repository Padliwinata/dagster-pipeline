from dagster import job, op


@op
def get_name_example():
    return "Dagster"


@op
def say_hello(name: str):
    print(f"Hello, {name}!")


@op
def say_goodbye(name: str):
    print(f"Goodbye, {name}!")


@job
def hello_world_job():
    name = get_name_example()
    say_hello(name)
    say_goodbye(name)


if __name__ == "__main__":
    hello_world_job.execute_in_process()