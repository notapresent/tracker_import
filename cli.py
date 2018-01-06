from gevent import monkey
monkey.patch_all()  # noqa
import logging
import click
import storage
import scraping
import grhttpc
import settings


logger = logging.getLogger(__name__)


@click.group()
@click.pass_context
def cli(ctx):
    init_logging(logging.INFO)
    ctx.obj['db'] = storage.init(settings.DATABASE_URL)


@cli.command()
@click.pass_context
def createdb(ctx):
    with storage.DBContext(ctx.obj['db']):
        storage.create_tables()


@cli.command()
@click.pass_context
def dropdb(ctx):
    with storage.DBContext(ctx.obj['db']):
        storage.drop_tables()


@cli.command()
@click.pass_context
def run(ctx):
    """Run scrape"""
    with storage.DBContext(ctx.obj['db']):
        httpc = grhttpc.GRequestsHttpClient(settings.CONCURRENCY)
        scraper = scraping.Scraper(httpc, ctx.obj['db'], settings)
        scraper.run()


@cli.command()
@click.pass_context
def newrun(ctx):
    """Reset database, then run scrape"""
    ctx.invoke(dropdb)
    ctx.invoke(createdb)
    ctx.invoke(run)


def init_logging(level=logging.INFO):
    """Set up logging parameters"""
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(name)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        level=level)

    warn_only = ['requests.packages.urllib3', 'urllib3', 'chardet']
    for pkgname in warn_only:
        logging.getLogger(pkgname).setLevel(logging.WARN)


if __name__ == '__main__':
    cli(obj={})
