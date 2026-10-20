"""Train the Isolation Forest on the labeled synthetic fleet and print evaluation metrics."""

from app.model import save, train

if __name__ == "__main__":
    model = train(verbose=True)
    save(model)
    print(f"saved model {model.version} (threshold {model.threshold:.4f})")
