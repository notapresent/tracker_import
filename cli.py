import click
from app import App
import settings


@click.group()
@click.pass_context
def cli(ctx):
    ctx.obj['app'] = App(settings)


@cli.command()
@click.pass_context
def run(ctx):
    """Run scrape"""
    ctx.obj['app'].run()


@cli.command()
@click.pass_context
def newrun(ctx):
    """Purge storage, then run scrape"""
    ctx.invoke(purge)
    ctx.invoke(run)


@cli.command()
@click.pass_context
def purge(ctx):
    """Purge storage"""
    ctx.obj['app'].purge()



if __name__ == '__main__':
    cli(obj={})
