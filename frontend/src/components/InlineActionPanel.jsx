import './InlineActionPanel.css';

export default function InlineActionPanel({
  title,
  description,
  onSubmit,
  onCancel,
  submitLabel,
  cancelLabel = 'Cancel',
  submitDisabled = false,
  children,
}) {
  return (
    <section className="ui-inline-panel" aria-label={title}>
      <div className="ui-inline-panel__header">
        <div>
          <h3>{title}</h3>
          {description ? <p>{description}</p> : null}
        </div>
      </div>

      <form className="ui-inline-form" onSubmit={onSubmit}>
        <div className="ui-inline-form__fields">{children}</div>
        <div className="ui-inline-form__actions">
          <button type="button" className="ui-action-button ui-action-button--ghost" onClick={onCancel}>
            {cancelLabel}
          </button>
          <button
            type="submit"
            className="ui-action-button ui-action-button--primary"
            disabled={submitDisabled}
          >
            {submitLabel}
          </button>
        </div>
      </form>
    </section>
  );
}
