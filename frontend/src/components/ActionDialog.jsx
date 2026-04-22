import './ActionDialog.css';

export default function ActionDialog({
  title,
  description,
  onClose,
  onSubmit,
  submitLabel = 'Save',
  submitDisabled = false,
  children,
}) {
  return (
    <div className="action-dialog-backdrop" role="presentation" onClick={onClose}>
      <div
        className="action-dialog"
        role="dialog"
        aria-modal="true"
        aria-label={title}
        onClick={(event) => event.stopPropagation()}
      >
        <div className="action-dialog__header">
          <div>
            <h2>{title}</h2>
            {description ? <p>{description}</p> : null}
          </div>
          <button type="button" className="action-dialog__close" onClick={onClose} aria-label="Close dialog">
            Close
          </button>
        </div>
        <form
          className="action-dialog__form"
          onSubmit={(event) => {
            event.preventDefault();
            onSubmit(event);
          }}
        >
          {children}
          <div className="action-dialog__actions">
            <button type="button" className="action-dialog__secondary" onClick={onClose}>
              Cancel
            </button>
            <button type="submit" className="action-dialog__primary" disabled={submitDisabled}>
              {submitLabel}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
