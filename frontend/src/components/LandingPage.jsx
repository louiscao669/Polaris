import { useState } from 'react';
import { Link } from 'react-router-dom';
import './LandingPage.css';
import atSeaImage from '../assets/At_SEA.png';
import atTradeImage from '../assets/At_TRADE.png';
import jockeyImage from '../assets/JOCKEY.png';

function LandingPage() {
  const slides = [
    { src: jockeyImage, alt: 'Polaris prediction market jockey artwork' },
    { src: atSeaImage, alt: 'Polaris experience at sea' },
    { src: atTradeImage, alt: 'Polaris trade market view' },
  ];
  const [activeSlide, setActiveSlide] = useState(0);
  const [expandedSlide, setExpandedSlide] = useState(null);

  const heroMessage = 'Prediction markets for real-time operational intelligence';

  const showPreviousSlide = () => {
    setActiveSlide((currentSlide) => (currentSlide === 0 ? slides.length - 1 : currentSlide - 1));
  };

  const showNextSlide = () => {
    setActiveSlide((currentSlide) => (currentSlide === slides.length - 1 ? 0 : currentSlide + 1));
  };

  return (
    <>
      <section className="landing-hero" aria-label="Polaris introduction">
        <div className="landing-hero__content">
          <p className="landing-hero__eyebrow">Prediction markets for organizations</p>
          <h1 className="landing-hero__title">Polaris</h1>
          <p className="landing-hero__message">{heroMessage}</p>
          <div className="landing-hero__actions">
            <Link className="landing-hero__cta" to="/signup">
              Try Now
            </Link>
            <p className="landing-hero__signin">
              Already have an account? <Link to="/signin">Sign in</Link>
            </p>
          </div>
        </div>

        <div className="landing-carousel" aria-label="Polaris highlights">
          <button
            type="button"
            className="landing-carousel__arrow landing-carousel__arrow--left"
            onClick={showPreviousSlide}
            aria-label="Show previous image"
          >
            &#8249;
          </button>

          <div className="landing-carousel__viewport">
            <div
              className="landing-carousel__track"
              style={{ transform: `translateX(-${activeSlide * 100}%)` }}
            >
              {slides.map((slide, index) => (
                <div className="landing-carousel__slide" key={slide.alt}>
                  <button
                    type="button"
                    className="landing-carousel__image-button"
                    onClick={() => setExpandedSlide(index)}
                    aria-label={`Open larger view of image ${index + 1}`}
                  >
                    <img className="landing-carousel__image" src={slide.src} alt={slide.alt} />
                  </button>
                </div>
              ))}
            </div>
          </div>

          <button
            type="button"
            className="landing-carousel__arrow landing-carousel__arrow--right"
            onClick={showNextSlide}
            aria-label="Show next image"
          >
            &#8250;
          </button>

          <div className="landing-carousel__dots" aria-label="Choose hero image">
            {slides.map((slide, index) => (
              <button
                key={slide.alt}
                type="button"
                className={`landing-carousel__dot${index === activeSlide ? ' is-active' : ''}`}
                onClick={() => setActiveSlide(index)}
                aria-label={`Show image ${index + 1}`}
                aria-pressed={index === activeSlide}
              />
            ))}
          </div>
        </div>
      </section>

      {expandedSlide !== null ? (
        <div
          className="landing-lightbox"
          role="dialog"
          aria-modal="true"
          aria-label="Expanded hero image"
          onClick={() => setExpandedSlide(null)}
        >
          <button
            type="button"
            className="landing-lightbox__close"
            onClick={() => setExpandedSlide(null)}
            aria-label="Close expanded image"
          >
            ×
          </button>
          <img
            className="landing-lightbox__image"
            src={slides[expandedSlide].src}
            alt={slides[expandedSlide].alt}
            onClick={(event) => event.stopPropagation()}
          />
        </div>
      ) : null}
    </>
  );
}

export default LandingPage;
