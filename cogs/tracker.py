from discord.ext import commands


class Tracker(commands.Cog):
    """Placeholder for future game tracking logic."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot


async def setup(bot: commands.Bot):
    await bot.add_cog(Tracker(bot))
