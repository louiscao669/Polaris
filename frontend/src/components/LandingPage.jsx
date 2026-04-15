import './LandingPage.css';

function LandingPage() {
  return (
    <div className="scroll-container">
      <section className="landing-section landing-section--hero blue-bg" aria-label="Introduction">
        <div className="container landing-section__inner">
          <h1>Introducing Polaris</h1>
          <p className="landing-section__tagline">A new way to bet for your organization.</p>
        </div>
      </section>

      <section className="landing-section landing-section--description bg-color" aria-label="Description">
        <div className="container">
          <h2 className="landing-section__heading">How it works</h2>
          <p>
            The system provides a platform for organizations to understand stakeholders&apos;
            expectations about issues of organizational interests. In a regular business setting,
            for instance, to measure the engineer team&apos;s sentiments on the time it will take
            to complete a given feature, organization leaders request information from managers
            directly responsible for the projects, creating a chain of information transmission
            that risks the information loss at different layers. However, our platform enables the
            leaders to create events such as the status of a feature, which contains markets that
            the team can directly bet on. All team members are invited and incentivized to make
            the most likely prediction. This eliminates social desirability bias and ensures
            honesty. It also removes the need for intermediary reports and enables transparent and
            data-driven analysis along with real-time events in the organization (e.g. an engineer
            leaving the team).
          </p>
          <p>
            Specifically, our system allows organization leaders to create events that are open
            only to users of certain roles within the organization and are given tokens to make the
            bets. In this exchange betting system, an individual places a black bet in a binary
            market at a token cost between 0 and 1. If another individual is willing to place a lay
            bet at the same odds such that the cost of the two bets adds up to 1, they will be
            matched, and the winner gets the amount, which is 1 token.
          </p>
          <p className="landing-section__cta-wrap">
            <a href="#signup" className="landing-section__cta">
              Continue to sign up
            </a>
          </p>
        </div>
      </section>
    </div>
  );
}

export default LandingPage;
