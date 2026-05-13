import matplotlib.pyplot as plt

from .field import Field


class FieldCollection:
    def __init__(self):
        self._fields = []

    def add(self, field):
        self._fields.append(field)

    def plot(self, filename):

        fig, ax = plt.subplots()

        for field in self._fields:

            cycles = field._repo.cycles()

            x_vals = []
            y_vals = []

            for t in cycles:
                try:
                    y_vals.append(field[t])
                    x_vals.append(t)
                except Exception:
                    continue

            label = field._variable_path
            ax.plot(x_vals, y_vals, label=label)

        ax.legend()
        ax.set_title("FieldCollection Plot")
        ax.set_xlabel("Time")
        ax.set_ylabel("Value")

        plt.savefig(filename)
        plt.close(fig)

        return filename
