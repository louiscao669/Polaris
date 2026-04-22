import * as React from 'react';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Checkbox from '@mui/material/Checkbox';
import CssBaseline from '@mui/material/CssBaseline';
import Divider from '@mui/material/Divider';
import FormControlLabel from '@mui/material/FormControlLabel';
import FormLabel from '@mui/material/FormLabel';
import FormControl from '@mui/material/FormControl';
import { Link as RouterLink, useNavigate } from 'react-router-dom';
import MuiLink from '@mui/material/Link';
import TextField from '@mui/material/TextField';
import Typography from '@mui/material/Typography';
import Stack from '@mui/material/Stack';
import MuiCard from '@mui/material/Card';
import { styled, useColorScheme } from '@mui/material/styles';
import AppTheme from '../shared-theme/AppTheme';
import { pollOperation, submitV2Operation, formatApiError } from '../../lib/api';
// import ColorModeSelect from '../shared-theme/ColorModeSelect';
import { GoogleIcon, FacebookIcon } from './components/CustomIcons';

const Card = styled(MuiCard)(({ theme }) => ({
  display: 'flex',
  flexDirection: 'column',
  alignSelf: 'center',
  width: '100%',
  padding: theme.spacing(4),
  gap: theme.spacing(2),
  margin: '0 auto',
  boxShadow:
    'hsla(220, 30%, 5%, 0.05) 0px 5px 15px 0px, hsla(220, 25%, 10%, 0.05) 0px 15px 35px -5px',
  [theme.breakpoints.up('sm')]: {
    width: '450px',
  },
  ...theme.applyStyles('dark', {
    boxShadow:
      'hsla(220, 30%, 5%, 0.5) 0px 5px 15px 0px, hsla(220, 25%, 10%, 0.08) 0px 15px 35px -5px',
  }),
}));

const SignUpContainer = styled(Stack)(({ theme }) => ({
  height: 'auto',
  minHeight: 'calc((1 - var(--template-frame-height, 0)) * 100dvh)',
  padding: theme.spacing(2, 2, 6),
  justifyContent: 'flex-start',
  [theme.breakpoints.up('sm')]: {
    padding: theme.spacing(4, 4, 8),
  },
  '&::before': {
    content: '""',
    display: 'block',
    position: 'absolute',
    zIndex: -1,
    inset: 0,
    backgroundImage:
      'radial-gradient(ellipse at 50% 50%, hsl(210, 100%, 97%), hsl(0, 0%, 100%))',
    backgroundRepeat: 'no-repeat',
    ...theme.applyStyles('dark', {
      backgroundImage:
        'radial-gradient(at 50% 50%, hsla(210, 100%, 16%, 0.5), hsl(220, 30%, 5%))',
    }),
  },
}));

function SignUpContent() {
  const navigate = useNavigate();
  const { mode, setMode } = useColorScheme();
  const [firstError, setfirstError] = React.useState(false);
  const [firstErrorMessage, setfirstErrorMessage] = React.useState('');
  const [lastError, setlastError] = React.useState(false);
  const [lastErrorMessage, setlastErrorMessage] = React.useState('');
  const [usernameError, setUsernameError] = React.useState(false);
  const [usernameErrorMessage, setUsernameErrorMessage] = React.useState('');
  const [emailError, setEmailError] = React.useState(false);
  const [emailErrorMessage, setEmailErrorMessage] = React.useState('');
  const [passwordError, setPasswordError] = React.useState(false);
  const [passwordErrorMessage, setPasswordErrorMessage] = React.useState('');
  const [loading, setLoading] = React.useState(false);
  const [serverError, setServerError] = React.useState('');

  React.useEffect(() => {
    if (mode !== 'dark') {
      setMode('dark');
    }
  }, [mode, setMode]);

  const validateInputs = () => {
    const first = document.getElementById('first');
    const last = document.getElementById('last');
    const username = document.getElementById('username');
    const email = document.getElementById('email');
    const password = document.getElementById('password');

    let isValid = true;

    if (!first.value || first.value.trim().length < 1) {
      setfirstError(true);
      setfirstErrorMessage('First name is required.');
      isValid = false;
    } else {
      setfirstError(false);
      setfirstErrorMessage('');
    }

    if (!last.value || last.value.trim().length < 1) {
      setlastError(true);
      setlastErrorMessage('Last name is required.');
      isValid = false;
    } else {
      setlastError(false);
      setlastErrorMessage('');
    }

    if (!username.value || username.value.trim().length < 1) {
      setUsernameError(true);
      setUsernameErrorMessage('Username is required.');
      isValid = false;
    } else {
      setUsernameError(false);
      setUsernameErrorMessage('');
    }

    if (!email.value || !/\S+@\S+\.\S+/.test(email.value)) {
      setEmailError(true);
      setEmailErrorMessage('Please enter a valid email address.');
      isValid = false;
    } else {
      setEmailError(false);
      setEmailErrorMessage('');
    }

    if (!password.value || password.value.length < 6) {
      setPasswordError(true);
      setPasswordErrorMessage('Password must be at least 6 characters long.');
      isValid = false;
    } else {
      setPasswordError(false);
      setPasswordErrorMessage('');
    }

    return isValid;
  };

  const handleSubmit = async (event) => {
    event.preventDefault();
    if (!validateInputs()) return;
    setLoading(true);
    setServerError('');
    const data = new FormData(event.currentTarget);
    const payload = Object.fromEntries(data);
    if (payload.age) {
      payload.age = Number(payload.age);
    } else {
      delete payload.age;
    }
    try {
      const result = await submitV2Operation('/user/account', {
        action: 'USER_SIGNUP',
        ...payload,
      });
      await pollOperation(result.operation_id);
      navigate('/signin', {
        state: {
          signupSuccess: 'Account created. Sign in to continue.',
          username: payload.username,
        },
      });
    } catch (error) {
      console.error('Signup error:', error);
      setServerError(error?.message || formatApiError(error, 'Sign up failed.'));
    }finally {
      setLoading(false);
    }
  };

  return (
    <>
      <CssBaseline enableColorScheme />
      {/* <ColorModeSelect sx={{ position: 'fixed', top: '1rem', right: '1rem' }} /> */}
      <SignUpContainer direction="column">
        <Card variant="outlined">
          <Typography component="p" variant="p" sx={{ fontWeight: 700, letterSpacing: 0.4 }}>
            Polaris
          </Typography>
          <Typography
            component="h1"
            variant="h4"
            sx={{ width: '100%', fontSize: 'clamp(2rem, 10vw, 2.15rem)', color: "#ffffff" }}
          >
            Sign up
          </Typography>
          <Box
            component="form"
            onSubmit={handleSubmit}
            sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}
          >
            <FormControl>
              <FormLabel htmlFor="first">First name</FormLabel>
              <TextField
                autoComplete="given-name"
                name="first"
                required
                fullWidth
                id="first"
                placeholder="Jon"
                error={firstError}
                helperText={firstErrorMessage}
                color={firstError ? 'error' : 'primary'}
              />
            </FormControl>
            <FormControl>
              <FormLabel htmlFor="last">Last name</FormLabel>
              <TextField
                autoComplete="family-name"
                name="last"
                required
                fullWidth
                id="last"
                placeholder="Snow"
                error={lastError}
                helperText={lastErrorMessage}
                color={lastError ? 'error' : 'primary'}
              />
            </FormControl>
            <FormControl>
              <FormLabel htmlFor="username">Username</FormLabel>
              <TextField
                autoComplete="username"
                name="username"
                required
                fullWidth
                id="username"
                placeholder="jonsnow"
                error={usernameError}
                helperText={usernameErrorMessage}
                color={usernameError ? 'error' : 'primary'}
              />
            </FormControl>
            <FormControl>
              <FormLabel htmlFor="email">Email</FormLabel>
              <TextField
                required
                fullWidth
                id="email"
                placeholder="your@email.com"
                name="email"
                autoComplete="email"
                variant="outlined"
                error={emailError}
                helperText={emailErrorMessage}
                color={emailError ? 'error' : 'primary'}
              />
            </FormControl>
            <FormControl>
              <FormLabel htmlFor="age">Age (optional)</FormLabel>
              <TextField
                fullWidth
                id="age"
                name="age"
                type="number"
                slotProps={{ htmlInput: { min: 0 } }}
                placeholder="25"
                autoComplete="off"
                variant="outlined"
              />
            </FormControl>
            <FormControl>
              <FormLabel htmlFor="password">Password</FormLabel>
              <TextField
                required
                fullWidth
                name="password"
                placeholder="••••••"
                type="password"
                id="password"
                autoComplete="new-password"
                variant="outlined"
                error={passwordError}
                helperText={passwordErrorMessage}
                color={passwordError ? 'error' : 'primary'}
              />
            </FormControl>
            <FormControlLabel
              control={<Checkbox value="allowExtraEmails" color="primary" />}
              label="I want to receive updates via email."
            />
            <Button
              type="submit"
              fullWidth
              variant="contained"
              disabled={loading}
            >
              {loading ? 'Signing up...' : 'Sign up'}
            </Button>
            {serverError ? (
              <Typography variant="body2" color="error" sx={{ textAlign: 'center' }}>
                {serverError}
              </Typography>
            ) : null}
            <Typography variant="body2" sx={{ textAlign: 'center', color: 'text.secondary' }}>
              Sign-up is processed through the live Polaris `/v2` operation queue.
            </Typography>
          </Box>
          <Divider>
            <Typography sx={{ color: 'text.secondary' }}>or</Typography>
          </Divider>
          <Box sx={{ display: 'flex', flexDirection: 'column', gap: 2 }}>
            <Button
              fullWidth
              variant="outlined"
              onClick={() => alert('Sign up with Google')}
              startIcon={<GoogleIcon />}
            >
              Sign up with Google
            </Button>
            <Button
              fullWidth
              variant="outlined"
              onClick={() => alert('Sign up with Facebook')}
              startIcon={<FacebookIcon />}
            >
              Sign up with Facebook
            </Button>
            <Typography sx={{ textAlign: 'center' }}>
              Already have an account?{' '}
              <MuiLink
                component={RouterLink}
                to="/signin"
                variant="body2"
                sx={{ alignSelf: 'center' }}
              >
                Sign in
              </MuiLink>
            </Typography>
          </Box>
        </Card>
      </SignUpContainer>
    </>
  );
}

export default function SignUp(props) {
  return (
    <AppTheme {...props}>
      <SignUpContent />
    </AppTheme>
  );
}
